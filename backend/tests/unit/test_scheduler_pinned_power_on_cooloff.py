"""Powering on a pinned job's offline printer cannot stall the whole queue.

The power-on waits for the printer to boot, up to `_power_on_wait_time`, and
the queue pass waits with it. The model branch already allowed one power-on
per pass and gave a printer that did not come up a cool-off (#2786); jobs
pinned to a printer had neither, so one printer that never answered cost every
30-second pass three minutes, for every such printer, and every other printer's
jobs started that much later.
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


def _plug(printer_id):
    return SimpleNamespace(
        id=printer_id,
        printer_id=printer_id,
        name=f"plug {printer_id}",
        auto_on=True,
        enabled=True,
        plug_type="tasmota",
        controls_printer_power=True,
    )


async def _reason(ctx, item_id):
    async with ctx.session_maker() as db:
        return (
            await db.execute(select(PrintQueueItem.waiting_reason).where(PrintQueueItem.id == item_id))
        ).scalar_one()


async def _pass(ctx, scheduler, power_on, waiting=None):
    patches = [
        patch("backend.app.services.print_scheduler.async_session", ctx.session_maker),
        patch("backend.app.core.database.async_session", ctx.session_maker),
        patch("backend.app.services.print_scheduler.printer_manager.is_connected", MagicMock(return_value=False)),
        patch("backend.app.services.print_scheduler.printer_manager.get_status", MagicMock(return_value=None)),
        patch(
            "backend.app.services.print_scheduler.ha_sensor_manager.blocked_printers",
            AsyncMock(return_value={}),
        ),
        patch(
            "backend.app.services.notification_service.notification_service.on_queue_job_waiting",
            waiting or AsyncMock(),
        ),
        patch.object(scheduler, "_check_auto_drying", AsyncMock()),
        patch.object(scheduler, "_wakeable_printer_ids", AsyncMock(return_value={1, 2})),
        patch.object(scheduler, "_get_smart_plugs", AsyncMock(side_effect=lambda _db, pid: [_plug(pid)])),
        patch.object(scheduler, "_power_on_and_wait", power_on),
        patch.object(scheduler, "_launch_uploads", MagicMock()),
    ]
    with ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        await scheduler.check_queue()


def _printers_tried(power_on):
    return [call.args[1] for call in power_on.await_args_list]


@pytest.mark.asyncio
async def test_one_power_on_per_pass_and_a_cool_off_after_it_fails(ctx):
    first = await _add(ctx, printer_id=1)
    second = await _add(ctx, printer_id=2)
    scheduler = PrintScheduler()
    power_on = AsyncMock(return_value=False)
    waiting = AsyncMock()

    await _pass(ctx, scheduler, power_on, waiting)
    assert _printers_tried(power_on) == [1]
    assert await _reason(ctx, second) == "Busy: P2S-02 (offline, powered on next)"
    assert [c.kwargs["printer_id"] for c in waiting.await_args_list] == [1]

    # Printer 1 is cooling off, so this pass tries printer 2 instead. Its
    # failure is news and is notified like printer 1's was.
    await _pass(ctx, scheduler, power_on, waiting)
    assert _printers_tried(power_on) == [1, 2]
    assert "could not power it on" in await _reason(ctx, first)
    assert "could not power it on" in await _reason(ctx, second)
    assert [c.kwargs["printer_id"] for c in waiting.await_args_list] == [1, 2]

    # Both cooling off: no pass waits on a boot until the cool-off ends, and
    # nothing is notified again.
    await _pass(ctx, scheduler, power_on, waiting)
    assert _printers_tried(power_on) == [1, 2]
    assert waiting.await_count == 2
