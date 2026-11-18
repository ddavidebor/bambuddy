"""The queue's rules, checked against the real scheduler over simulated hours.

Each bug fixed before this file existed had its own test, and each test built
the one situation that bug needed. That proves the bug is gone; it says
nothing about the next one. This file runs the real ``check_queue`` against
fake printers and a fake clock, feeds it a random stream of jobs, drags,
printers dropping offline and dispatches that go wrong in the ways real ones
do, and after every pass checks the rules a user relies on:

* order: a printer goes to the first job in the queue that may run on it;
* no idle printer while a job that may run on it waits;
* no job is sent to a printer that is printing, offline or backing off;
* a scheduled job's printer is not given to a job that would still be running
  at its time (``SCHEDULED`` runs only).

The rules are written once, in ``_Rules``, independently of the scheduler's
code. A violation prints the seed; ``SIM_SEEDS=500`` runs more of them.
"""

import os
import random
import time as real_time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import backend.app.models  # noqa: F401 - populate Base.metadata
from backend.app.core.database import Base
from backend.app.models.library import LibraryFile
from backend.app.models.print_queue import PrintQueueItem
from backend.app.models.printer import Printer
from backend.app.services.bambu_mqtt import PrinterState
from backend.app.services.print_scheduler import PrintScheduler
from backend.app.services.queue_position import move_queue_items

PRINTERS = {1: "P1S", 2: "P1S", 3: "P1S", 4: "X1C"}
START = datetime(2026, 3, 2, 8, 0, tzinfo=timezone.utc)
UNKNOWN_DURATION = timedelta(hours=24)  # an unknown print time is assumed to overrun
SEEDS = int(os.environ.get("SIM_SEEDS", "15"))
STEPS = 60


class _Clock:
    def __init__(self):
        self.now = START
        self._mono = 1_000_000.0

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        self._mono += seconds

    def monotonic(self) -> float:
        return self._mono


@dataclass
class _Printer:
    model: str
    connected: bool = True
    state: str = "IDLE"
    busy_until: datetime | None = None
    job: int | None = None  # queue item printing on it, None for a print started by hand


class _FakePrinters:
    """The parts of ``printer_manager`` the scheduler reads, backed by ``_Printer``s."""

    def __init__(self, printers: dict[int, _Printer]):
        self.printers = printers

    def is_connected(self, pid):
        return pid in self.printers and self.printers[pid].connected

    def get_status(self, pid):
        p = self.printers.get(pid)
        return None if p is None else PrinterState(connected=p.connected, state=p.state)

    def get_all_statuses(self):
        return {pid: self.get_status(pid) for pid in self.printers}

    def get_model(self, pid):
        return self.printers[pid].model if pid in self.printers else None

    def is_awaiting_plate_clear(self, _pid):
        return False

    def last_known_trays(self, _pid):
        return {}

    def get_client(self, _pid):
        return None

    @contextmanager
    def uploading(self, _pid):
        yield

    def __getattr__(self, name):
        # Side effects the scheduler fires and forgets (print user, drying).
        return MagicMock(name=f"printer_manager.{name}")


class _FakeDatetime(datetime):
    clock: _Clock

    @classmethod
    def now(cls, tz=None):
        now = cls.clock.now
        return now if tz is not None else now.replace(tzinfo=None)


@dataclass
class _Job:
    id: int
    order: tuple
    printer_id: int | None
    target_model: str | None
    scheduled: datetime | None
    manual_start: bool
    duration: timedelta


@dataclass
class _Rules:
    """What should happen, from the user's point of view. Not the scheduler's code."""

    clock: _Clock
    printers: dict[int, _Printer]
    jobs: list[_Job]
    printing_on: set[int]
    backoff: dict[int, float]
    scheduled_rule: bool
    violations: list[str] = field(default_factory=list)

    def free(self, pid: int) -> bool:
        p = self.printers[pid]
        return (
            p.connected
            and p.state in ("IDLE", "FINISH", "FAILED")
            and pid not in self.printing_on
            and self.backoff.get(pid, 0) <= self.clock.monotonic()
        )

    def due(self, job: _Job) -> bool:
        return not job.manual_start and (job.scheduled is None or job.scheduled <= self.clock.now)

    def may_take(self, job: _Job, pid: int) -> bool:
        if job.target_model is not None:
            return self.printers[pid].model == job.target_model
        return job.printer_id == pid

    def reserved_against(self, job: _Job, pid: int) -> bool:
        """Starting *job* on *pid* would run into a scheduled job's time there."""
        if not self.scheduled_rule:
            return False
        end = self.clock.now + job.duration
        return any(
            other.id != job.id
            and other.target_model is None
            and other.printer_id == pid
            and not other.manual_start
            and other.scheduled is not None
            and self.clock.now < other.scheduled < end
            for other in self.jobs
        )

    def runnable(self, job: _Job, pid: int) -> bool:
        return self.due(job) and self.may_take(job, pid) and not self.reserved_against(job, pid)

    def check(self, dispatched: dict[int, int]) -> None:
        by_id = {job.id: job for job in self.jobs}
        claimed = set(dispatched.values())
        now = self.clock.now.strftime("%H:%M")
        for job_id, pid in dispatched.items():
            job = by_id[job_id]
            if not self.free(pid):
                self.violations.append(f"{now} job {job_id} sent to printer {pid}, which was not free")
                continue
            if not self.due(job):
                self.violations.append(f"{now} job {job_id} sent to printer {pid} before its time or while staged")
                continue
            if not self.may_take(job, pid):
                self.violations.append(f"{now} job {job_id} sent to printer {pid}, which it is not for")
                continue
            if self.reserved_against(job, pid):
                self.violations.append(f"{now} job {job_id} sent to printer {pid}, running into a scheduled job there")
                continue
            ahead = [
                other.id
                for other in self.jobs
                if other.order < job.order and other.id not in dispatched and self.runnable(other, pid)
            ]
            if ahead:
                self.violations.append(f"{now} job {job_id} took printer {pid} ahead of {ahead}")
        for pid in self.printers:
            if pid in claimed or not self.free(pid):
                continue
            waiting = [job.id for job in self.jobs if job.id not in dispatched and self.runnable(job, pid)]
            if waiting:
                self.violations.append(f"{now} printer {pid} left idle while {waiting} could run on it")


class _Sim:
    def __init__(self, seed: int, scheduled: bool, session_maker):
        self.rng = random.Random(seed)
        self.scheduled = scheduled
        self.session_maker = session_maker
        self.clock = _Clock()
        self.printers = {pid: _Printer(model) for pid, model in PRINTERS.items()}
        self.fake = _FakePrinters(self.printers)
        self.scheduler = PrintScheduler()
        self.violations: list[str] = []
        self.dispatches = 0

    async def add_job(self, db) -> None:
        rng = self.rng
        model_job = rng.random() < 0.5
        pid = rng.choice(list(PRINTERS))
        lib = LibraryFile(
            filename="job.gcode.3mf",
            file_path="/library/job.gcode.3mf",
            file_size=10,
            file_type="gcode.3mf",
            file_metadata={"sliced_for_model": PRINTERS[pid]},
        )
        db.add(lib)
        await db.flush()
        top = (await db.execute(select(PrintQueueItem.position).order_by(PrintQueueItem.position.desc()))).first()
        scheduled = None
        if self.scheduled and not model_job and rng.random() < 0.3:
            scheduled = (self.clock.now + timedelta(minutes=rng.randint(5, 360))).replace(tzinfo=None)
        db.add(
            PrintQueueItem(
                status="pending",
                position=(top[0] if top else 0) + 1,
                printer_id=None if model_job else pid,
                target_model=PRINTERS[pid] if model_job else None,
                library_file_id=lib.id,
                print_time_seconds=None if rng.random() < 0.1 else rng.randint(10, 240) * 60,
                scheduled_time=scheduled,
                manual_start=rng.random() < 0.05,
            )
        )

    async def snapshot(self, db) -> tuple[list[_Job], set[int]]:
        rows = (await db.execute(select(PrintQueueItem))).scalars().all()
        jobs = [
            _Job(
                id=r.id,
                order=(r.position, r.id),
                printer_id=r.printer_id,
                target_model=r.target_model,
                scheduled=r.scheduled_time.replace(tzinfo=timezone.utc) if r.scheduled_time else None,
                manual_start=r.manual_start,
                duration=timedelta(seconds=r.print_time_seconds) if r.print_time_seconds else UNKNOWN_DURATION,
            )
            for r in rows
            if r.status == "pending"
        ]
        return jobs, {r.printer_id for r in rows if r.status == "printing"}

    async def between_passes(self, db) -> None:
        rng = self.rng
        self.clock.advance(rng.choice([30, 60, 120, 300, 900]))
        for pid, p in self.printers.items():
            if p.busy_until and p.busy_until <= self.clock.now:
                if p.job is not None:
                    item = await db.get(PrintQueueItem, p.job)
                    item.status = "completed"
                    item.completed_at = self.clock.now.replace(tzinfo=None)
                p.state, p.busy_until, p.job = "FINISH", None, None
            if rng.random() < 0.03:
                p.connected = not p.connected
        for _ in range(rng.choice([0, 0, 1, 1, 2])):
            await self.add_job(db)
        pending = (
            (
                await db.execute(
                    select(PrintQueueItem.id)
                    .where(PrintQueueItem.status == "pending")
                    .order_by(PrintQueueItem.position, PrintQueueItem.id)
                )
            )
            .scalars()
            .all()
        )
        if len(pending) >= 2 and rng.random() < 0.2:
            moving, anchor = rng.sample(list(pending), 2)
            await move_queue_items(db, [moving], anchor, rng.random() < 0.5, lambda _item: True)
        await db.commit()

    async def apply(self, db, dispatched: dict[int, int]) -> None:
        """What each upload did, as the real dispatch paths leave the row and printer."""
        rng = self.rng
        for job_id, pid in dispatched.items():
            item = await db.get(PrintQueueItem, job_id)
            roll = rng.random()
            printer = self.printers[pid]
            if roll < 0.80:
                item.status = "printing"
                item.started_at = self.clock.now.replace(tzinfo=None)
                duration = item.print_time_seconds or 3600
                printer.state, printer.job = "RUNNING", job_id
                printer.busy_until = self.clock.now + timedelta(seconds=duration)
            elif roll < 0.87:
                pass  # upload pool full: the row is left as the selection wrote it
            elif roll < 0.94:
                # Someone started a print by hand before the upload (#2598).
                printer.state, printer.job = "RUNNING", None
                printer.busy_until = self.clock.now + timedelta(minutes=rng.randint(10, 120))
            else:
                # The printer refused the upload (#3210, #3409).
                self.scheduler._upload_backoff[pid] = self.clock.monotonic() + 300
                if item.target_model:
                    item.printer_id = None
        await db.commit()

    async def run_pass(self) -> dict[int, int]:
        launched = MagicMock()
        _FakeDatetime.clock = self.clock
        fake_time = SimpleNamespace(**{k: getattr(real_time, k) for k in dir(real_time) if not k.startswith("_")})
        fake_time.monotonic = self.clock.monotonic
        s = self.scheduler
        patches = [
            patch("backend.app.services.print_scheduler.async_session", self.session_maker),
            patch("backend.app.core.database.async_session", self.session_maker),
            patch("backend.app.services.print_scheduler.printer_manager", self.fake),
            patch("backend.app.services.print_scheduler.datetime", _FakeDatetime),
            patch("backend.app.services.print_scheduler.time", fake_time),
            patch(
                "backend.app.services.print_scheduler.ha_sensor_manager.blocked_printers", AsyncMock(return_value={})
            ),
            patch("backend.app.services.print_scheduler.notification_service", AsyncMock()),
            patch.object(s, "_check_auto_drying", AsyncMock()),
            patch.object(s, "_check_filament_low", AsyncMock()),
            patch.object(s, "_check_stock_forecast", AsyncMock()),
            patch.object(s, "_check_scheduled_dryings", AsyncMock()),
            patch.object(s, "_apply_keep_warm", AsyncMock()),
            patch.object(s, "_ensure_ams_mapping", AsyncMock(return_value=None)),
            patch.object(s, "_block_on_filament_deficit", AsyncMock(return_value=False)),
            patch.object(s, "_launch_uploads", launched),
        ]
        with ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            await s.check_queue()
        if not launched.called:
            return {}
        ids, item_printers, _limit = launched.call_args[0]
        return {i: item_printers[i] for i in ids}

    async def run(self, steps: int) -> list[str]:
        for _ in range(steps):
            async with self.session_maker() as db:
                await self.between_passes(db)
                jobs, printing_on = await self.snapshot(db)
            rules = _Rules(
                clock=self.clock,
                printers={pid: _Printer(**vars(p)) for pid, p in self.printers.items()},
                jobs=jobs,
                printing_on=printing_on,
                backoff=dict(self.scheduler._upload_backoff),
                scheduled_rule=self.scheduled,
            )
            dispatched = await self.run_pass()
            self.dispatches += len(dispatched)
            rules.check(dispatched)
            self.violations += rules.violations
            async with self.session_maker() as db:
                await self.apply(db, dispatched)
        return self.violations


async def _simulate(seed: int, scheduled: bool) -> _Sim:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as db:
        for pid, model in PRINTERS.items():
            db.add(
                Printer(
                    id=pid,
                    name=f"{model}-{pid}",
                    serial_number=f"SIM{pid:04d}",
                    ip_address=f"10.9.0.{pid}",
                    access_code="x",
                    model=model,
                    is_active=True,
                )
            )
        await db.commit()
    sim = _Sim(seed, scheduled, session_maker)
    try:
        await sim.run(STEPS)
    finally:
        await engine.dispose()
    return sim


async def _check_seeds(scheduled: bool) -> None:
    failures = []
    dispatches = 0
    for seed in range(SEEDS):
        sim = await _simulate(seed, scheduled)
        dispatches += sim.dispatches
        if sim.violations:
            failures.append(f"seed {seed}: " + "; ".join(sim.violations[:3]))
    assert dispatches > SEEDS * 5, "the simulation dispatched almost nothing; it is not exercising the scheduler"
    assert not failures, "\n".join(failures[:10])


@pytest.mark.asyncio
async def test_queue_rules_hold_without_schedules():
    await _check_seeds(scheduled=False)


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="a scheduled job does not hold its printer: a job that overruns its time starts first",
)
async def test_queue_rules_hold_with_scheduled_jobs():
    await _check_seeds(scheduled=True)
