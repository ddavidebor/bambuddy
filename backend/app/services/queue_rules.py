"""The rules that decide which queued job a printer takes, and when.

Pure functions over queue rows, shared by the scheduler, which applies them,
and the projection, which runs them forward to say when each job will start.
One copy, so the timeline cannot drift from what the queue does.

They read only plain attributes (``scheduled_time``, ``manual_start``,
``print_time_seconds``, ``position``, ``id``, ``been_jumped``, ``printer_id``,
``target_model``, ``variants``), so a projection's stand-in rows work as well
as ORM ones.
"""

from datetime import datetime, timedelta, timezone

# A job with no print-time estimate is taken to run this long when deciding
# whether it would still be printing at a scheduled job's time: long enough
# that it never slips in front of one, short enough not to hold a printer for a
# job scheduled days out.
UNKNOWN_PRINT_TIME_FOR_RESERVATION = timedelta(hours=24)


def aware_utc(value: datetime | None) -> datetime | None:
    """A stored naive-UTC datetime as an aware one."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)


def is_due_scheduled(item, now: datetime) -> bool:
    """A scheduled job whose time has come, and that is not waiting for Start."""
    sched = aware_utc(item.scheduled_time)
    return sched is not None and sched <= now and not item.manual_start


def queue_order_key(item, *, sjf: bool, now: datetime) -> tuple:
    """The order jobs get printers in.

    Queue position, or Shortest-Job-First when that is on: shortest first, no
    estimate last. Its starvation guard puts a job that was jumped ahead of the
    rest, and among those it is first come, first served by position. Ordering
    them by print time again let a long jumped job be overtaken by every
    shorter one jumped after it, which is the starvation the guard is for.

    A scheduled job whose time has come goes ahead of all of them, earliest
    time first: it was promised that time, and a printer busy then must not hand
    it to whatever sits higher once it frees.
    """
    head = (0, aware_utc(item.scheduled_time)) if is_due_scheduled(item, now) else (1,)
    if sjf and item.been_jumped:
        rest = (0, item.position, item.id)
    elif sjf:
        seconds = item.print_time_seconds
        rest = (1, seconds is None, seconds or 0, item.position, item.id)
    else:
        rest = (item.position, item.id)
    return head, rest


def reserved_printer(item, now: datetime) -> tuple[int, datetime] | None:
    """The printer *item* holds and the time it holds it for, if any.

    Only a job pinned to a printer and scheduled for later holds one. An "Any
    <model>" job would need free printers counted per model over time; it gets
    the due ordering instead.
    """
    sched = aware_utc(item.scheduled_time)
    if (
        item.printer_id is not None
        and not (item.target_model or item.variants)
        and not item.manual_start
        and sched is not None
        and sched > now
    ):
        return item.printer_id, sched
    return None


def runs_into_reservation(item, upcoming: list[datetime], now: datetime) -> bool:
    """Whether starting *item* now would still be running at one of *upcoming*.

    *upcoming* are the start times of jobs scheduled on the printer. The earlier
    schedule wins: a scheduled job is held only by one scheduled before it, so
    two whose times overlap cannot hold each other off.
    """
    if not upcoming:
        return False
    seconds = item.print_time_seconds
    end = now + (timedelta(seconds=seconds) if seconds else UNKNOWN_PRINT_TIME_FOR_RESERVATION)
    own = aware_utc(item.scheduled_time)
    return any(now < t < end and (own is None or t < own) for t in upcoming)
