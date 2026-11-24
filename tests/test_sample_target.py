"""Consistency tests for the sample target's counters and exposition."""

from __future__ import annotations

import threading

import pytest

from app.histogram import parse_metrics_text
from app.sample_target import (
    METRIC_NAME,
    TargetState,
    render_metrics,
)


def _state() -> TargetState:
    return TargetState("sample-a", [0.1, 0.5, 1.0])


def _view_text(view) -> str:
    return render_metrics(view)


def test_snapshot_is_internally_consistent_after_writes():
    state = _state()
    state.inject(0.2, 3)
    view = state.take_snapshot()
    try:
        # Writers keep moving the live counters...
        state.inject(0.3, 5)
        state.inject(2.0, 2)
        state.reset()
        # ...but the view still describes one instant of the old process.
        text = _view_text(view)
    finally:
        state.release_snapshot()

    snapshot = parse_metrics_text(text, METRIC_NAME, 0.0)
    assert snapshot.process_id == view.run_id
    assert snapshot.count == 3
    assert snapshot.buckets[0.5] == 3
    assert snapshot.buckets[float("inf")] == 3


def test_outstanding_copies_never_exceed_in_flight_scrapes():
    state = _state()
    state.inject(0.2, 1)
    held = [state.take_snapshot() for _ in range(4)]
    try:
        assert state.state_payload()["outstanding_snapshot_copies"] == 4

        errors: list[BaseException] = []

        def writer() -> None:
            try:
                for index in range(2_000):
                    if index % 97 == 0:
                        state.reset()
                    else:
                        state.inject(0.05, 1)
            except BaseException as exc:  # pragma: no cover - defensive
                errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(4)]
        for thread in threads:
            thread.start()
        # While writers hammer the counters, retention stays at exactly the
        # number of in-flight scrapes -- old bucket copies are never queued.
        assert state.state_payload()["outstanding_snapshot_copies"] == 4
        for thread in threads:
            thread.join()
        assert not errors
        assert state.state_payload()["outstanding_snapshot_copies"] == 4
    finally:
        for _ in held:
            state.release_snapshot()
    assert state.state_payload()["outstanding_snapshot_copies"] == 0


def test_each_snapshot_has_one_run_id_and_consistent_buckets():
    state = _state()
    views = []
    for _ in range(50):
        state.inject(0.2, 10)
        views.append(state.take_snapshot())
        state.inject(0.7, 3)  # falls into no finite bucket here
        state.reset()
        state.inject(0.2, 1)

    for view in views:
        snapshot = parse_metrics_text(_view_text(view), METRIC_NAME, 0.0)
        # parse_metrics_text itself rejects mixed identities and
        # non-monotonic / +Inf-inconsistent bucket series.
        assert snapshot.process_id == view.run_id
        assert snapshot.count == view.total_count
    for view in views:
        state.release_snapshot()


def test_restart_and_inject_complete_while_render_is_blocked(monkeypatch):
    """Writers never wait for response encoding.

    ``/metrics`` copies the histogram and releases the lock *before* the
    body is produced, so blocked encodings cannot block inject / restart.
    """

    state = _state()
    state.inject(0.2, 1)
    release = threading.Event()
    entered = threading.Event()
    view = state.take_snapshot()

    def slow_encoder() -> None:
        entered.set()
        assert release.wait(timeout=5)
        render_metrics(view)

    encoder = threading.Thread(target=slow_encoder)
    encoder.start()
    assert entered.wait(timeout=5)

    # While the "encoding" holds the snapshot, writes succeed immediately.
    state.inject(0.3, 2)
    state.reset()
    assert state.state_payload()["outstanding_snapshot_copies"] == 1

    release.set()
    encoder.join(timeout=5)
    state.release_snapshot()
    assert state.state_payload()["outstanding_snapshot_copies"] == 0
    assert parse_metrics_text(_view_text(view), METRIC_NAME, 0.0).count == 1
