"""The projection runs the queue's own rules forward to say when each job starts."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backend.app.services.queue_projection import ProjectedPrinter, project

NOW = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)


def job(id, *, position=None, printer=None, model=None, hours=1.0, at=None, manual=False, phase=None, variants=()):
    return SimpleNamespace(
        id=id,
        position=id if position is None else position,
        printer_id=printer,
        target_model=model,
        target_location=None,
        variants=[SimpleNamespace(target_model=m) for m in variants],
        print_time_seconds=None if hours is None else int(hours * 3600),
        scheduled_time=(NOW + at).replace(tzinfo=None) if at is not None else None,
        manual_start=manual,
        been_jumped=False,
        dispatch_phase=phase,
    )


def printer(id, model="P1S", free_in=timedelta(0)):
    return ProjectedPrinter(id=id, model=model, location=None, free_at=None if free_in is None else NOW + free_in)


def starts(items, printers, sjf=False):
    return {i: (p.printer_id, p.start - NOW) for i, p in project(items, printers, NOW, sjf=sjf).items()}


def test_jobs_on_a_printer_follow_each_other_in_queue_order():
    assert starts([job(2, printer=1, position=2), job(1, printer=1, position=1, hours=2)], [printer(1)]) == {
        1: (1, timedelta(0)),
        2: (1, 2 * H),
    }


def test_a_printer_still_printing_starts_its_next_job_when_it_frees():
    assert starts([job(1, printer=1)], [printer(1, free_in=90 * timedelta(minutes=1))]) == {
        1: (1, 90 * timedelta(minutes=1))
    }


def test_a_scheduled_job_holds_its_printer_and_a_short_job_fits_before_it():
    items = [job(1, printer=1, hours=3), job(2, printer=1, hours=0.5), job(3, printer=1, at=2 * H, hours=1)]
    assert starts(items, [printer(1)]) == {
        2: (1, timedelta(0)),
        3: (1, 2 * H),
        1: (1, 3 * H),
    }


def test_an_any_model_job_takes_the_printer_that_frees_first():
    items = [job(1, model="P1S")]
    printers = [printer(1, free_in=2 * H), printer(2, free_in=H), printer(3, model="X1C")]
    assert starts(items, printers) == {1: (2, H)}


def test_a_cross_model_job_may_take_any_of_its_models():
    assert starts([job(1, variants=("X1C", "P1S"))], [printer(4, model="X1C", free_in=H), printer(1)]) == {
        1: (1, timedelta(0))
    }


def test_a_due_scheduled_job_goes_before_jobs_higher_in_the_queue():
    items = [job(1, printer=1, position=1), job(2, printer=1, position=2, at=-H)]
    assert starts(items, [printer(1)]) == {2: (1, timedelta(0)), 1: (1, H)}


def test_after_a_print_of_unknown_length_nothing_more_is_projected():
    items = [job(1, printer=1, hours=None), job(2, printer=1)]
    assert starts(items, [printer(1)]) == {1: (1, timedelta(0))}


def test_staged_jobs_and_unavailable_printers_are_not_projected():
    items = [job(1, printer=1, manual=True), job(2, printer=2)]
    assert starts(items, [printer(1), printer(2, free_in=None)]) == {}


def test_a_job_being_uploaded_starts_now_on_its_printer():
    items = [job(1, printer=1, position=1), job(2, printer=1, position=2, phase="uploading")]
    assert starts(items, [printer(1)]) == {2: (1, timedelta(0)), 1: (1, H)}
