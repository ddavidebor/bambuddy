"""An "Any <model>" job returns to the pool when the pass that picked a printer
for it does not dispatch it.

The model-based branch writes the printer it picked onto the row before the
dispatch runs. When the dispatch then does not happen (the upload pool was
full, or the printer turned busy before the upload) the row kept that printer,
and the next pass took it for a job pinned to it: it waited behind whatever
that printer was doing while other printers of the model stood idle.
"""

from contextlib import ExitStack
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
from backend.app.services.print_scheduler import PrintScheduler


@pytest.fixture
async def ctx():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        for pid in (1, 2):
            db.add(
                Printer(
                    id=pid,
                    name=f"P2S-0{pid}",
                    serial_number=f"P2S000{pid}",
                    ip_address=f"10.0.0.{pid}",
                    access_code="x",
                    model="P2S",
                    is_active=True,
                )
            )
        await db.commit()

    try:
        yield SimpleNamespace(session_maker=session_maker)
    finally:
        await engine.dispose()


async def _add(ctx, *, printer_id=None, target_model=None, manual_start=False):
    async with ctx.session_maker() as db:
        lib = LibraryFile(
            filename="job.gcode.3mf",
            file_path="/library/job.gcode.3mf",
            file_size=10,
            file_type="gcode.3mf",
            file_metadata={"sliced_for_model": "P2S"},
        )
        db.add(lib)
        await db.flush()
        item = PrintQueueItem(
            status="pending",
            position=1,
            printer_id=printer_id,
            target_model=target_model,
            library_file_id=lib.id,
            manual_start=manual_start,
        )
        db.add(item)
        await db.commit()
        return item.id


async def _printer_of(ctx, item_id):
    async with ctx.session_maker() as db:
        return (await db.execute(select(PrintQueueItem.printer_id).where(PrintQueueItem.id == item_id))).scalar_one()


async def _run(ctx, scheduler, *, idle_printers):
    """One check_queue pass; returns the ids it launched uploads for."""
    launched = MagicMock()
    patches = [
        patch("backend.app.services.print_scheduler.async_session", ctx.session_maker),
        patch("backend.app.core.database.async_session", ctx.session_maker),
        patch("backend.app.services.print_scheduler.printer_manager.is_connected", MagicMock(return_value=True)),
        patch("backend.app.services.print_scheduler.printer_manager.get_status", MagicMock(return_value=None)),
        patch(
            "backend.app.services.print_scheduler.printer_manager.is_awaiting_plate_clear",
            MagicMock(return_value=False),
        ),
        patch(
            "backend.app.services.print_scheduler.ha_sensor_manager.blocked_printers",
            AsyncMock(return_value={}),
        ),
        patch(
            "backend.app.services.notification_service.notification_service.on_queue_job_waiting",
            AsyncMock(),
        ),
        patch(
            "backend.app.services.notification_service.notification_service.on_queue_job_assigned",
            AsyncMock(),
        ),
        patch.object(
            scheduler,
            "_is_printer_idle",
            MagicMock(side_effect=lambda pid, *_a, **_k: pid in idle_printers),
        ),
        patch.object(scheduler, "_check_auto_drying", AsyncMock()),
        patch.object(scheduler, "_ensure_ams_mapping", AsyncMock(return_value=None)),
        patch.object(scheduler, "_block_on_filament_deficit", AsyncMock(return_value=False)),
        patch.object(scheduler, "_get_smart_plugs", AsyncMock(return_value=[])),
        patch.object(scheduler, "_launch_uploads", launched),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        await scheduler.check_queue()
    return list(launched.call_args[0][0]) if launched.called else []


@pytest.mark.asyncio
async def test_a_model_job_left_on_a_busy_printer_goes_to_an_idle_one(ctx):
    # Assigned to printer 2 by an earlier pass that did not dispatch it; printer
    # 2 is now printing something else, printer 1 is free.
    job = await _add(ctx, printer_id=2, target_model="P2S")

    assert await _run(ctx, PrintScheduler(), idle_printers={1}) == [job]
    assert await _printer_of(ctx, job) == 1


@pytest.mark.asyncio
async def test_a_released_model_job_waits_unassigned_when_no_printer_is_free(ctx):
    job = await _add(ctx, printer_id=2, target_model="P2S")

    assert await _run(ctx, PrintScheduler(), idle_printers=set()) == []
    assert await _printer_of(ctx, job) is None


@pytest.mark.asyncio
async def test_a_job_the_printer_may_still_start_keeps_its_printer(ctx):
    """The start watchdog put it back after the file reached printer 2. Sending
    it to printer 1 could print it twice if printer 2 starts late."""
    job = await _add(ctx, printer_id=2, target_model="P2S")
    scheduler = PrintScheduler()
    scheduler._sent_unstarted_print[job] = (2, "job.gcode.3mf", None)

    assert await _run(ctx, scheduler, idle_printers={1}) == []
    assert await _printer_of(ctx, job) == 2


@pytest.mark.asyncio
async def test_a_staged_model_job_keeps_its_printer(ctx):
    """Held there by the filament checks, which move it themselves (#3137)."""
    job = await _add(ctx, printer_id=2, target_model="P2S", manual_start=True)

    await _run(ctx, PrintScheduler(), idle_printers={1})
    assert await _printer_of(ctx, job) == 2


@pytest.mark.asyncio
async def test_a_pinned_job_is_not_released(ctx):
    job = await _add(ctx, printer_id=2)

    assert await _run(ctx, PrintScheduler(), idle_printers={1}) == []
    assert await _printer_of(ctx, job) == 2
