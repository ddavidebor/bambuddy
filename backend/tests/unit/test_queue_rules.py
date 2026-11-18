"""The ordering the scheduler and the projection share."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backend.app.services.queue_rules import queue_order_key

NOW = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)


def job(id, *, hours=1.0, jumped=False, at=None, manual=False):
    return SimpleNamespace(
        id=id,
        position=id,
        print_time_seconds=None if hours is None else int(hours * 3600),
        been_jumped=jumped,
        scheduled_time=(NOW + at).replace(tzinfo=None) if at is not None else None,
        manual_start=manual,
    )


def order(items, sjf):
    return [j.id for j in sorted(items, key=lambda j: queue_order_key(j, sjf=sjf, now=NOW))]


def test_position_order_without_sjf():
    assert order([job(2, hours=0.1), job(1, hours=9)], sjf=False) == [1, 2]


def test_sjf_runs_shortest_first_and_unknown_last():
    assert order([job(1, hours=None), job(2, hours=3), job(3, hours=1)], sjf=True) == [3, 2, 1]


def test_jumped_jobs_go_first_and_first_come_first_served():
    """By print time again, a long jumped job is overtaken by every shorter one
    jumped after it, the starvation the guard exists to stop."""
    items = [job(1, hours=8, jumped=True), job(2, hours=0.5, jumped=True), job(3, hours=0.1)]
    assert order(items, sjf=True) == [1, 2, 3]


def test_a_due_scheduled_job_goes_before_everything_earliest_first():
    items = [job(1, jumped=True), job(2, at=-timedelta(minutes=5)), job(3, at=-timedelta(hours=1))]
    assert order(items, sjf=True) == [3, 2, 1]
    assert order(items, sjf=False) == [3, 2, 1]


def test_a_staged_or_future_scheduled_job_keeps_its_place():
    items = [job(1), job(2, at=timedelta(hours=1)), job(3, at=-timedelta(hours=1), manual=True)]
    assert order(items, sjf=False) == [1, 2, 3]
