"""A queue row says how far its dispatch has got, and a restart cannot lose that.

"uploading" while the file goes out, "sent" from the start command until the
printer is seen to start, nothing once it has. The start watchdog that confirms
the start lives in memory, so after a restart a "sent" row had no one to settle
it: the stranded-item sweep closed it as cancelled once the printer sat idle,
and a job that never printed left the queue as if someone had cancelled it.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.app.models.library import LibraryFile
from backend.app.models.print_queue import PrintQueueItem
from backend.app.services.bambu_mqtt import PrinterState
from backend.app.services.print_scheduler import (
    DISPATCH_MAX_ATTEMPTS,
    UNCONFIRMED_SEND_GRACE_SECONDS,
    PrintScheduler,
)
from backend.tests.scheduler_helpers import add_job, two_p2s_printers


@pytest.fixture
async def ctx():
    async with two_p2s_printers() as ctx:
        with patch("backend.app.services.print_scheduler.async_session", ctx.session_maker):
            yield ctx


async def _row(ctx, item_id) -> PrintQueueItem:
    async with ctx.session_maker() as db:
        return await db.get(PrintQueueItem, item_id)


async def _sent(ctx, *, minutes_ago: float, attempts: int = 0, subtask: str | None = "100") -> int:
    item_id = await add_job(ctx, printer_id=1)
    async with ctx.session_maker() as db:
        item = await db.get(PrintQueueItem, item_id)
        sent = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
        item.status = "printing"
        item.dispatch_phase = "sent"
        item.sent_at = sent.replace(tzinfo=None)
        item.started_at = sent.replace(tzinfo=None)
        item.dispatch_attempts = attempts
        item.sent_subtask_id = subtask
        await db.commit()
    return item_id


def _printer(state: str, connected: bool = True, subtask: str | None = "100"):
    return patch(
        "backend.app.services.print_scheduler.printer_manager.get_status",
        MagicMock(return_value=PrinterState(connected=connected, state=state, subtask_id=subtask)),
    )


STALE = UNCONFIRMED_SEND_GRACE_SECONDS / 60 + 1


class TestThePhaseFollowsTheDispatch:
    @pytest.mark.asyncio
    async def test_a_claim_marks_it_uploading_and_its_end_clears_that(self, ctx):
        scheduler = PrintScheduler()
        item_id = await add_job(ctx, printer_id=1)

        async with ctx.session_maker() as db:
            assert await scheduler._claim_for_dispatch(db, item_id, 1)
        assert (await _row(ctx, item_id)).dispatch_phase == "uploading"

        async with ctx.session_maker() as db:
            await scheduler._clear_dispatch_claim(db, item_id)
        row = await _row(ctx, item_id)
        assert (row.dispatching_at, row.dispatch_phase) == (None, None)

    @pytest.mark.asyncio
    async def test_the_end_of_a_claim_keeps_sent(self, ctx):
        """The claim ends after the row turned printing; "sent" is the watchdog's to clear."""
        item_id = await _sent(ctx, minutes_ago=0)
        async with ctx.session_maker() as db:
            await PrintScheduler()._clear_dispatch_claim(db, item_id)
        assert (await _row(ctx, item_id)).dispatch_phase == "sent"

    @pytest.mark.asyncio
    async def test_a_seen_start_confirms_it(self, ctx):
        item_id = await _sent(ctx, minutes_ago=0)
        await PrintScheduler._confirm_started(item_id)
        assert (await _row(ctx, item_id)).dispatch_phase is None


class TestASendNoWatchdogIsWatching:
    @pytest.mark.asyncio
    async def test_goes_back_to_the_queue_when_the_printer_is_still_on_its_old_subtask(self, ctx):
        item_id = await _sent(ctx, minutes_ago=STALE)

        with _printer("IDLE"), patch("backend.app.services.print_batch.refresh_batches_after", AsyncMock()):
            await PrintScheduler()._reconcile_unconfirmed_sends()

        row = await _row(ctx, item_id)
        assert (row.status, row.dispatch_phase, row.sent_at, row.dispatch_attempts) == ("pending", None, None, 1)

    @pytest.mark.asyncio
    async def test_fails_once_out_of_attempts(self, ctx):
        item_id = await _sent(ctx, minutes_ago=STALE, attempts=DISPATCH_MAX_ATTEMPTS - 1)

        with _printer("FINISH"), patch("backend.app.services.print_batch.refresh_batches_after", AsyncMock()):
            await PrintScheduler()._reconcile_unconfirmed_sends()

        row = await _row(ctx, item_id)
        assert row.status == "failed"
        assert "never started printing" in row.error_message

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stored", ["100", None], ids=["ran_since", "nothing_stored"])
    async def test_is_not_requeued_without_proof_it_never_ran(self, ctx, stored):
        """Bambuddy down while the print ran and finished: the printer is idle on
        a new subtask. Requeueing would print the job again; it is left to the
        stranded-item sweep, which closes it with the printer's status."""
        item_id = await _sent(ctx, minutes_ago=STALE, subtask=stored)

        with (
            _printer("FINISH", subtask="200"),
            patch("backend.app.services.print_batch.refresh_batches_after", AsyncMock()),
        ):
            await PrintScheduler()._reconcile_unconfirmed_sends()

        row = await _row(ctx, item_id)
        assert (row.status, row.dispatch_phase, row.dispatch_attempts) == ("printing", None, 0)

    @pytest.mark.asyncio
    async def test_is_confirmed_when_the_printer_is_printing(self, ctx):
        item_id = await _sent(ctx, minutes_ago=STALE)

        with _printer("RUNNING"), patch("backend.app.services.print_batch.refresh_batches_after", AsyncMock()):
            await PrintScheduler()._reconcile_unconfirmed_sends()

        row = await _row(ctx, item_id)
        assert (row.status, row.dispatch_phase) == ("printing", None)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("case", ["recent", "watched", "offline"])
    async def test_is_left_alone_while_it_proves_nothing(self, ctx, case):
        item_id = await _sent(ctx, minutes_ago=1 if case == "recent" else STALE)
        scheduler = PrintScheduler()
        if case == "watched":
            scheduler._watched_sends.add(item_id)

        with _printer("IDLE", connected=case != "offline"):
            await scheduler._reconcile_unconfirmed_sends()

        row = await _row(ctx, item_id)
        assert (row.status, row.dispatch_phase) == ("printing", "sent")

    @pytest.mark.asyncio
    async def test_is_not_closed_as_cancelled_by_the_stranded_sweep(self, ctx):
        item_id = await _sent(ctx, minutes_ago=STALE)
        scheduler = PrintScheduler()

        with (
            _printer("IDLE"),
            patch("backend.app.services.print_scheduler._STRANDED_PRINTING_GRACE_SECONDS", 0.0),
        ):
            await scheduler._close_stranded_printing_items()
            await scheduler._close_stranded_printing_items()

        assert (await _row(ctx, item_id)).status == "printing"


class TestJobsQueuedWithoutAPrintTime:
    @pytest.mark.asyncio
    async def test_get_their_files_time_at_startup(self, ctx):
        """Otherwise each counts as a day long and waits behind any job scheduled within it."""
        item_id = await add_job(ctx, printer_id=1)
        async with ctx.session_maker() as db:
            item = await db.get(PrintQueueItem, item_id)
            item.print_time_seconds = None
            library_file = await db.get(LibraryFile, item.library_file_id)
            library_file.file_metadata = {**(library_file.file_metadata or {}), "print_time_seconds": 5400}
            await db.commit()

        await PrintScheduler._backfill_print_times()

        assert (await _row(ctx, item_id)).print_time_seconds == 5400
