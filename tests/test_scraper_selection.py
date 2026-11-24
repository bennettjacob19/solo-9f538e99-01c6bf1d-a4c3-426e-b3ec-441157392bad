"""Deterministic tests for the two-scrape selection rules."""

import time

from app.histogram import HistogramSnapshot
from app.scraper import FRESHNESS_WINDOW, MIN_SAMPLE_GAP, ScrapeRecord, ScrapeStore


def _record(at: float, pid: str, buckets=(0.1, 0.5, 1.0), counts=(1, 2, 3),
            total=4, ok=True, error=None) -> ScrapeRecord:
    if not ok:
        return ScrapeRecord(ok=False, at=at, error=error)
    snap = HistogramSnapshot(
        metric_name="m",
        buckets=dict(zip(buckets, counts)),
        count=total,
        sum_value=1.0,
        process_id=pid,
        scrape_seq=None,
        scraped_at=at,
    )
    return ScrapeRecord(ok=True, at=at, snapshot=snap)


def _store(records):
    store = ScrapeStore({"i1": "http://x/metrics"})
    store.instances["i1"].history = records
    return store


def test_select_happy_path():
    now = 100.0
    store = _store([_record(now - 2.0, "p1"), _record(now - 0.9, "p1",
                                                       counts=(2, 4, 6), total=8)])
    outcome = store._select_sample(store.instances["i1"], now)
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4
    assert outcome.sample_times == (now - 2.0, now - 0.9)


def test_select_insufficient_scrapes():
    store = _store([_record(1.0, "p1")])
    assert store._select_sample(store.instances["i1"], 2.0) == "insufficient_scrapes"


def test_select_ignores_failed_scrapes():
    now = 10.0
    store = _store([
        _record(now - 2.1, "p1"),
        _record(now - 1.5, "x", ok=False, error="boom"),
        _record(now - 1.0, "p1", counts=(2, 4, 6), total=8),
    ])
    outcome = store._select_sample(store.instances["i1"], now)
    # The two successful records are 1.1s apart and both fresh.
    assert not isinstance(outcome, str)
    assert outcome.delta.total_requests == 4


def test_select_stale_scrape():
    now = time.time()
    store = _store([
        _record(now - FRESHNESS_WINDOW - 0.1, "p1"),
        _record(now - FRESHNESS_WINDOW - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "stale_scrape"


def test_select_samples_too_close():
    now = time.time()
    store = _store([
        _record(now - 0.9, "p1"),
        _record(now - 0.05, "p1"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "samples_too_close"


def test_select_process_restarted():
    now = time.time()
    store = _store([
        _record(now - 2.0, "p1"),
        _record(now - 0.9, "p2"),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_select_counter_reset_treated_as_restart():
    now = time.time()
    # Same process label but the newer counters are lower -> reset.
    store = _store([
        _record(now - 2.0, "p1", counts=(5, 9, 10), total=10),
        _record(now - 0.9, "p1", counts=(1, 2, 3), total=4),
    ])
    assert store._select_sample(store.instances["i1"], now) == "process_restarted"


def test_gap_constant_matches_requirement():
    assert MIN_SAMPLE_GAP >= 1.0
    assert FRESHNESS_WINDOW <= 3.0


# --------------------------------------------------------------------------- #
# Integrity quarantine
# --------------------------------------------------------------------------- #

def _seq_records(records):
    """Assign monotonically increasing scrape sequence numbers."""
    for index, record in enumerate(records):
        record.seq = index
    return records


def test_inconsistent_increment_quarantines_until_two_new_scrapes():
    now = time.time()
    # Same process id and valid per-snapshot buckets, but the cumulative
    # increment falls as the boundary rises -> contradiction.
    bad_earlier = _record(now - 1.6, "p1", counts=(10, 30, 40), total=40)
    bad_later = _record(now - 0.6, "p1", counts=(20, 35, 60), total=60)
    store = _store(_seq_records([bad_earlier, bad_later]))

    outcome = store._select_sample(store.instances["i1"], now)
    assert outcome == "inconsistent_increment"
    state = store.instances["i1"]
    assert state.last_issue is not None
    assert state.last_issue.code == "cumulative_delta_decreases"
    assert state.valid_from_seq == 1
    # The offending snapshots are discarded: not even one usable scrape.
    assert store._select_sample(state, now) == "insufficient_scrapes"
    assert "累计增量随边界下降" in store.missing_reason("i1")

    # One new scrape is not enough to clear the barrier.
    state.history.append(
        _record(now + 0.5, "p1", counts=(30, 45, 70), total=70)
    )
    state.history[-1].seq = 2
    assert store._select_sample(state, now + 1.0) == "insufficient_scrapes"
    assert "需等待两次新的合格同进程抓取" in store.missing_reason("i1")

    # Two new same-process scrapes form a fresh, consistent sample.
    state.history.append(
        _record(now + 1.6, "p1", counts=(40, 55, 80), total=80)
    )
    state.history[-1].seq = 3
    fresh = store._select_sample(state, now + 2.0)
    assert not isinstance(fresh, str)
    assert fresh.delta.total_requests == 10


def test_stats_reports_instance_and_reason():
    now = time.time()
    store = _store(_seq_records([
        _record(now - 1.6, "p1", counts=(10, 30, 40), total=40),
        _record(now - 0.6, "p1", counts=(20, 35, 60), total=60),
    ]))
    assert store._select_sample(store.instances["i1"], now) == "inconsistent_increment"
    stats = store.stats()
    info = stats["instances"]["i1"]
    assert info["last_issue"]["code"] == "cumulative_delta_decreases"
    assert "累计增量随边界下降" in info["last_issue"]["reason"]
    assert info["issues"]
    assert info["usable_scrapes"] == 0


def test_mixed_snapshot_is_not_recorded_as_success(monkeypatch):
    # A scrape rejected by the parser must not enter the sample selection.
    from app.histogram import MetricsParseError
    import app.scraper as scraper_module

    class FakeResponse:
        text = "broken"

        def raise_for_status(self):
            return None

    class FakeClient:
        async def get(self, url):
            return FakeResponse()

    def boom(text, metric, at):
        raise MetricsParseError("no buckets")

    monkeypatch.setattr(scraper_module, "parse_metrics_text", boom)
    store = ScrapeStore({"i1": "http://x/metrics"})
    store._client = FakeClient()
    import asyncio

    asyncio.run(store._scrape_once(store.instances["i1"]))
    state = store.instances["i1"]
    assert state.total_attempts == 1
    assert state.total_failures == 1
    assert state.usable_successes() == []
