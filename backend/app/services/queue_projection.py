"""When each queued job will start, and on which printer.

The queue's rules (``queue_rules``) run forward in time from what the printers
are doing now: the printer that frees first takes the first job in queue order
that may run on it, as ``check_queue`` would, and the clock moves to the next
printer to free or the next scheduled time. The timeline draws this, so what
it shows is what the scheduler will do, not a second guess at it.

It is an estimate. Print times are the slicer's, a printer with no remaining
time or one that is offline or waiting for its plate to be cleared is left
out, and holds the scheduler only finds out about when it gets there
(filament, sensors, a failed previous print) are not modelled. A job that
cannot be placed is not projected rather than projected wrong.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.app.models.print_queue import PrintQueueItem
from backend.app.models.printer import Printer
from backend.app.models.settings import Settings
from backend.app.services.queue_rules import (
    aware_utc,
    queue_order_key,
    reserved_printer,
    runs_into_reservation,
)


@dataclass
class ProjectedPrinter:
    id: int
    model: str | None
    location: str | None
    free_at: datetime | None  # None: not taking work we can predict
    wear: tuple = ()  # where "Any <model>" jobs try it, per the dispatch order setting (#2915)


@dataclass
class Projection:
    item_id: int
    printer_id: int
    start: datetime
    end: datetime | None  # None: no print-time estimate


def _may_run_on(item, printer: ProjectedPrinter) -> bool:
    if item.target_model or item.variants:
        models = {v.target_model for v in item.variants} if item.variants else {item.target_model}
        model_ok = any(m and printer.model and m.lower() == printer.model.lower() for m in models)
        return model_ok and (not item.target_location or item.target_location == printer.location)
    return item.printer_id == printer.id


def project(items: list, printers: list[ProjectedPrinter], now: datetime, *, sjf: bool) -> dict[int, Projection]:
    """Projected start of every job in *items* that can be placed.

    At each moment a printer frees, the jobs are walked in queue order and each
    takes the first free printer it may run on, least worn first, as a
    ``check_queue`` pass does. *items* are pending rows; staged ones are left
    out, as the scheduler never starts them on its own.
    """
    waiting = [it for it in items if not it.manual_start]
    free = {p.id: p.free_at for p in printers if p.free_at is not None}
    by_id = {p.id: p for p in printers}
    result: dict[int, Projection] = {}

    def place(it, pid: int, at: datetime) -> None:
        seconds = it.print_time_seconds
        end = at + timedelta(seconds=seconds) if seconds else None
        result[it.id] = Projection(item_id=it.id, printer_id=pid, start=at, end=end)
        if end is None:
            # What follows a print of unknown length on this printer is unknown.
            del free[pid]
        else:
            free[pid] = end

    # A job whose upload is running has its printer already: it starts there now.
    for it in [it for it in waiting if getattr(it, "dispatch_phase", None) == "uploading"]:
        waiting.remove(it)
        if it.printer_id in free:
            place(it, it.printer_id, now)

    # Each round places a job or retires or moves on a printer, so this bounds it.
    for _ in range(4 * (len(waiting) + len(printers)) + 4):
        if not waiting or not free:
            break
        at = min(free.values())
        available = sorted((pid for pid, f in free.items() if f <= at), key=lambda pid: by_id[pid].wear)

        reservations: dict[int, list[datetime]] = {}
        for it in waiting:
            if held := reserved_printer(it, at):
                reservations.setdefault(held[0], []).append(held[1])

        taken: set[int] = set()
        for it in sorted(waiting, key=lambda it: queue_order_key(it, sjf=sjf, now=at)):
            sched = aware_utc(it.scheduled_time)
            if sched is not None and sched > at:
                continue
            pid = next(
                (
                    pid
                    for pid in available
                    if pid not in taken
                    and _may_run_on(it, by_id[pid])
                    and not runs_into_reservation(it, reservations.get(pid, []), at)
                ),
                None,
            )
            if pid is not None:
                taken.add(pid)
                waiting.remove(it)
                place(it, pid, at)

        # A printer nothing could start on waits for the next scheduled time,
        # the only thing that can change that; with none coming, it is done.
        upcoming = [s for it in waiting if (s := aware_utc(it.scheduled_time)) is not None and s > at]
        for pid in available:
            if pid not in taken:
                if upcoming:
                    free[pid] = min(upcoming)
                else:
                    del free[pid]
    return result


def _free_at(printer_id: int, now: datetime, require_plate_clear: bool) -> datetime | None:
    from backend.app.services import plate_clear
    from backend.app.services.print_scheduler import _ACTIVE_PRINT_STATES
    from backend.app.services.printer_manager import printer_manager

    state = printer_manager.get_status(printer_id)
    if state is None or not printer_manager.is_connected(printer_id):
        return None
    if state.state in ("IDLE", "FINISH", "FAILED"):
        if plate_clear.applies(printer_id, require_plate_clear) and printer_manager.is_awaiting_plate_clear(printer_id):
            return None
        return now
    if state.state in _ACTIVE_PRINT_STATES and state.state != "PAUSE" and state.remaining_time > 0:
        return now + timedelta(minutes=state.remaining_time)
    return None


async def project_queue(db: AsyncSession, now: datetime | None = None) -> dict[int, Projection]:
    """The projection for the whole pending queue, from the printers' live state."""
    now = now or datetime.now(timezone.utc)
    settings = {
        row.key: row.value
        for row in (
            await db.execute(select(Settings).where(Settings.key.in_(("queue_shortest_first", "require_plate_clear"))))
        ).scalars()
    }
    sjf = (settings.get("queue_shortest_first") or "").lower() == "true"
    require_plate_clear = (settings.get("require_plate_clear") or "").lower() == "true"

    from backend.app.services.print_scheduler import _order_printers, scheduler

    rows = list((await db.execute(select(Printer).where(Printer.is_active.is_(True)))).scalars())
    # The order the scheduler tries printers in for an "Any <model>" job.
    order = await scheduler._get_model_dispatch_order(db)
    last_finished = (
        await scheduler._last_finished_by_printer(db, [p.id for p in rows])
        if order == "least_recently_used" and rows
        else None
    )
    rank = {p.id: i for i, p in enumerate(_order_printers(rows, order, last_finished))}
    printers = [
        ProjectedPrinter(
            id=p.id,
            model=p.model,
            location=p.location,
            free_at=_free_at(p.id, now, require_plate_clear),
            wear=(rank[p.id],),
        )
        for p in rows
    ]
    items = list(
        (
            await db.execute(
                select(PrintQueueItem)
                .where(PrintQueueItem.status == "pending")
                .options(selectinload(PrintQueueItem.variants))
                .order_by(PrintQueueItem.position, PrintQueueItem.id)
            )
        ).scalars()
    )
    # One held for the user (filament to load, a door to shut) waits until
    # they act, so it takes no printer in the projection. Busy is not that.
    held = {it.id for it in items if it.waiting_reason and not scheduler._is_busy_only(it.waiting_reason)}
    return project([it for it in items if it.id not in held], printers, now, sjf=sjf)
