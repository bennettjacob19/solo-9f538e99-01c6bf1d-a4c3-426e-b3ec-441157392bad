"""Shared scrape store and the verification orchestration.

A single background task scrapes every configured instance on a fixed
cadence (>= 1s apart per instance).  All concurrent ``/check`` requests read
from the same in-memory store, so checks share samples and one request
timing out never disturbs another.

Evidence discipline
-------------------
A scrape is only usable when the *whole* exposition belongs to one process
at one instant (see :func:`app.histogram.parse_metrics_text`).  On top of
that this module enforces, per instance:

* the two selected scrapes must share one process identity;
* the cumulative increment between them must be a valid cumulative series
  (non-negative, non-decreasing with the boundary, bounded by ``_count``);
* whenever a scrape or an increment is found inconsistent, the instance is
  *quarantined*: both snapshots involved are discarded and no sample is
  accepted until **two new** successful same-process scrapes have arrived.

The quarantine barrier is a monotone scrape sequence number, so it survives
history trimming.  ``/check`` keeps waiting for the caller's original
deadline and reports the instance plus the reason if the evidence never
arrives.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx

from .histogram import (
    DeltaIntegrityError,
    HistogramDelta,
    HistogramSnapshot,
    MetricsParseError,
    SnapshotIntegrityError,
    compute_delta,
    parse_metrics_text,
)

logger = logging.getLogger(__name__)

METRIC_NAME = "http_request_duration_seconds"
# Same-instance scrapes must be at least this far apart (seconds).
MIN_SAMPLE_GAP = 1.0
# A scrape is only usable for this long after it happened.
FRESHNESS_WINDOW = 3.0
# How often the background loop scrapes each instance.
SCRAPE_INTERVAL = 1.05
# Keep the last N scrapes per instance (only the newest two are used).
HISTORY_LIMIT = 8
HTTP_TIMEOUT = 2.0

# Machine readable reasons returned by the sample selector.
REASON_INSUFFICIENT = "insufficient_scrapes"
REASON_STALE = "stale_scrape"
REASON_TOO_CLOSE = "samples_too_close"
REASON_RESTARTED = "process_restarted"
REASON_SNAPSHOT_INCONSISTENT = "inconsistent_snapshot"
REASON_INCREMENT_INCONSISTENT = "inconsistent_increment"

# Human readable rendering of every integrity failure cause.
ISSUE_TEXT = {
    "mixed_process_identity": "同一次抓取混入多个进程标识",
    "missing_process_identity": "抓取缺少进程标识",
    "non_monotonic_buckets": "桶计数随边界下降",
    "bucket_exceeds_total": "桶计数超过总数",
    "inf_mismatch": "总数与 +Inf 桶不符",
    "duplicate_bucket": "同一边界重复出现",
    "negative_bucket": "桶计数为负",
    "negative_total": "总数或总和为负",
    "missing_finite_buckets": "缺少有限桶边界",
    "different_metric": "两次抓取属于不同指标",
    "bucket_layout_changed": "两次抓取的桶边界不同",
    "process_restarted": "进程标识发生变化（疑似重启）",
    "counter_reset": "累计计数回退（疑似重启）",
    "cumulative_delta_decreases": "两份合格快照相减后累计增量随边界下降",
    "delta_exceeds_total": "桶增量超过总数增量",
    "invalid_metrics": "指标文本无法解析",
}


@dataclass
class ScrapeRecord:
    ok: bool
    at: float
    snapshot: HistogramSnapshot | None = None
    error: str | None = None
    seq: int = 0


@dataclass
class InstanceIssue:
    """One integrity problem observed on an instance."""

    at: float
    code: str
    detail: str
    scrape_seq: int
    count: int = 1

    @property
    def reason(self) -> str:
        return ISSUE_TEXT.get(self.code, self.detail)

    def as_dict(self) -> dict:
        return {
            "at": self.at,
            "code": self.code,
            "reason": self.reason,
            "detail": self.detail,
            "scrape_seq": self.scrape_seq,
            "occurrences": self.count,
        }


@dataclass
class InstanceState:
    name: str
    url: str
    history: list[ScrapeRecord] = field(default_factory=list)
    total_attempts: int = 0
    total_successes: int = 0
    total_failures: int = 0
    # Next scrape sequence number to hand out (monotone, never reused).
    next_seq: int = 0
    # Records with ``seq <= valid_from_seq`` can no longer be used.
    valid_from_seq: int = -1
    # Wall-clock time of the newest fully validated sample; an integrity
    # issue at or before this time has been resolved by fresh evidence.
    resolved_at: float = float("-inf")
    issues: list[InstanceIssue] = field(default_factory=list)

    @property
    def latest(self) -> ScrapeRecord | None:
        return self.history[-1] if self.history else None

    @property
    def last_issue(self) -> InstanceIssue | None:
        return self.issues[-1] if self.issues else None

    def usable_successes(self) -> list[ScrapeRecord]:
        """Successful scrapes that are still allowed past the barrier."""
        return [r for r in self.history if r.ok and r.seq > self.valid_from_seq]


@dataclass
class ReadySample:
    """The two validated scrapes for one instance."""

    earlier: ScrapeRecord
    later: ScrapeRecord
    delta: HistogramDelta

    @property
    def sample_times(self) -> tuple[float, float]:
        return self.earlier.at, self.later.at


class ScrapeStore:
    def __init__(
        self,
        instances: dict[str, str],
        metric_name: str = METRIC_NAME,
        scrape_interval: float = SCRAPE_INTERVAL,
        min_gap: float = MIN_SAMPLE_GAP,
        freshness: float = FRESHNESS_WINDOW,
    ) -> None:
        self.instances = {
            name: InstanceState(name=name, url=url) for name, url in instances.items()
        }
        self.metric_name = metric_name
        self.scrape_interval = scrape_interval
        self.min_gap = min_gap
        self.freshness = freshness
        self._cond = asyncio.Condition()
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._task is None:
            self._client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
            self._task = asyncio.create_task(self._scrape_loop(), name="scrape-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ #
    # Scrape side
    # ------------------------------------------------------------------ #

    def _record_issue(
        self,
        state: InstanceState,
        code: str,
        detail: str,
        at: float,
        scrape_seq: int,
    ) -> None:
        """Record an integrity problem and quarantine the instance.

        Any snapshot that could have contributed to the bad evidence is
        discarded; the next sample must be built from two *new* successful
        same-process scrapes.
        """
        had_new_success = any(
            r.ok and r.seq > state.valid_from_seq for r in state.history
        )
        new_barrier = max(
            state.valid_from_seq, scrape_seq, self._latest_success_seq(state)
        )
        state.valid_from_seq = new_barrier

        last = state.last_issue
        if last is not None and last.code == code and not had_new_success:
            # Same class of problem observed again while already quarantined
            # and no fresh successful scrape has arrived yet; keep one entry
            # but refresh its timestamp/detail instead of spamming /stats.
            last.at = at
            last.detail = detail
            last.count += 1
            return

        issue = InstanceIssue(at=at, code=code, detail=detail, scrape_seq=scrape_seq)
        state.issues.append(issue)
        if len(state.issues) > 8:
            del state.issues[: len(state.issues) - 8]
        logger.warning("instance %s quarantined: %s (%s)", state.name, code, detail)

    async def _scrape_once(self, state: InstanceState) -> None:
        assert self._client is not None
        state.total_attempts += 1
        seq = state.next_seq
        state.next_seq += 1
        try:
            response = await self._client.get(state.url)
            response.raise_for_status()
            snapshot = parse_metrics_text(
                response.text, self.metric_name, time.time()
            )
            record = ScrapeRecord(
                ok=True, at=snapshot.scraped_at, snapshot=snapshot, seq=seq
            )
            state.total_successes += 1
        except (httpx.HTTPError, MetricsParseError, ValueError) as exc:
            record = ScrapeRecord(
                ok=False,
                at=time.time(),
                error=f"{type(exc).__name__}: {exc}",
                seq=seq,
            )
            state.total_failures += 1
            if isinstance(exc, SnapshotIntegrityError):
                self._record_issue(
                    state,
                    exc.code,
                    exc.detail,
                    at=record.at,
                    # The failed scrape may also have mutated the target's
                    # counters; invalidate it and everything before it.
                    scrape_seq=seq,
                )
        except Exception as exc:  # defensive: one bad scrape must not kill the loop
            logger.exception("unexpected scrape failure for %s", state.name)
            record = ScrapeRecord(
                ok=False,
                at=time.time(),
                error=f"{type(exc).__name__}: {exc}",
                seq=seq,
            )
            state.total_failures += 1

        state.history.append(record)
        if len(state.history) > HISTORY_LIMIT:
            del state.history[: len(state.history) - HISTORY_LIMIT]

    @staticmethod
    def _latest_success_seq(state: InstanceState) -> int:
        for record in reversed(state.history):
            if record.ok:
                return record.seq
        return -1

    async def _scrape_loop(self) -> None:
        while True:
            await asyncio.gather(
                *(self._scrape_once(state) for state in self.instances.values()),
                return_exceptions=True,
            )
            async with self._cond:
                self._cond.notify_all()
            await asyncio.sleep(self.scrape_interval)

    # ------------------------------------------------------------------ #
    # Read side
    # ------------------------------------------------------------------ #

    def sample_status(self, name: str, now: float | None = None) -> ReadySample | str:
        """Public wrapper around :meth:`_select_sample` for observability."""
        return self._select_sample(
            self.instances[name], time.time() if now is None else now
        )

    def _select_sample(self, state: InstanceState, now: float) -> ReadySample | str:
        """Validate the two most recent usable scrapes for one instance.

        Returns a :class:`ReadySample`, or a machine-readable reason string.
        On an inconsistent increment the offending snapshots are discarded
        and the instance is quarantined until two further scrapes arrive.
        """

        records = state.usable_successes()
        if len(records) < 2:
            return REASON_INSUFFICIENT
        earlier, later = records[-2], records[-1]
        assert earlier.snapshot is not None and later.snapshot is not None

        if now - later.at > self.freshness:
            return REASON_STALE
        if now - earlier.at > self.freshness:
            return REASON_STALE
        if later.at - earlier.at < self.min_gap:
            return REASON_TOO_CLOSE
        if (
            earlier.snapshot.process_id is None
            or later.snapshot.process_id is None
            or earlier.snapshot.process_id != later.snapshot.process_id
        ):
            return REASON_RESTARTED
        try:
            delta = compute_delta(earlier.snapshot, later.snapshot)
        except DeltaIntegrityError as exc:
            # Two individually valid snapshots do not form a valid
            # increment: distrust both and demand two fresh scrapes.
            self._record_issue(
                state,
                exc.code,
                exc.detail,
                at=later.at,
                scrape_seq=later.seq,
            )
            if exc.code in ("counter_reset", "process_restarted"):
                # A counter reset with an unchanged label means a restart
                # slipped past the process identity.
                return REASON_RESTARTED
            return REASON_INCREMENT_INCONSISTENT
        # A valid sample now exists: any issue that predates it is resolved.
        state.resolved_at = max(state.resolved_at, later.at)
        return ReadySample(earlier=earlier, later=later, delta=delta)

    async def wait_for_samples(
        self, deadline: float
    ) -> tuple[dict[str, ReadySample], list[str], dict[str, ReadySample | str]]:
        """Wait until every instance has a valid two-scrape sample or timeout.

        Concurrent calls block on the same condition variable and therefore
        share every scrape the background task produces.  The third element
        is the per-instance outcome evaluated at the moment the wait gave up
        (identical to the second element's cause map when nothing is missing).
        """

        while True:
            ready: dict[str, ReadySample] = {}
            missing: list[str] = []
            outcomes: dict[str, ReadySample | str] = {}
            wall_now = time.time()
            for name, state in self.instances.items():
                outcome = self._select_sample(state, wall_now)
                outcomes[name] = outcome
                if isinstance(outcome, ReadySample):
                    ready[name] = outcome
                else:
                    missing.append(name)

            if not missing:
                return ready, [], outcomes
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ready, missing, outcomes

            async with self._cond:
                try:
                    await asyncio.wait_for(
                        self._cond.wait(),
                        timeout=max(0.0, deadline - time.monotonic()),
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

    # ------------------------------------------------------------------ #
    # Observability
    # ------------------------------------------------------------------ #

    def issue_is_active(self, name: str) -> bool:
        """True while a recorded integrity problem still blocks evidence."""

        state = self.instances[name]
        issue = state.last_issue
        if issue is None:
            return False
        return issue.at > state.resolved_at

    def missing_reason(self, name: str, outcome: str | None = None) -> str:
        """Chinese explanation for why an instance has no usable sample."""

        state = self.instances[name]
        if outcome is None:
            current = self._select_sample(state, time.time())
            outcome = current if isinstance(current, str) else None
        if outcome is None:
            return "ready"

        issue = state.last_issue
        base = {
            REASON_INSUFFICIENT: "成功抓取不足两次",
            REASON_STALE: "抓取超出近 3 秒新鲜窗口",
            REASON_TOO_CLOSE: "两次抓取间隔不足 1 秒",
            REASON_RESTARTED: "两次抓取来自不同进程（疑似重启）",
            REASON_SNAPSHOT_INCONSISTENT: "抓取快照内部矛盾",
            REASON_INCREMENT_INCONSISTENT: "两份快照相减后增量矛盾",
        }.get(outcome, outcome)
        if self.issue_is_active(name) and issue is not None:
            # The barrier is still in force, so "not enough scrapes" really
            # means "waiting for two new valid same-process scrapes".
            return f"{issue.reason}（{issue.detail}），需等待两次新的合格同进程抓取"
        return base

    def stats(self) -> dict:
        result: dict = {"instances": {}}
        now = time.time()
        for name, state in self.instances.items():
            last_ok = next(
                (r for r in reversed(state.history) if r.ok), None
            )
            usable = state.usable_successes()
            result["instances"][name] = {
                "url": state.url,
                "scrape_attempts": state.total_attempts,
                "scrape_successes": state.total_successes,
                "scrape_failures": state.total_failures,
                "history_len": len(state.history),
                "latest_scrape_at": state.latest.at if state.latest else None,
                "latest_ok": state.latest.ok if state.latest else None,
                "latest_error": state.latest.error if state.latest else None,
                "last_success_at": last_ok.at if last_ok else None,
                "last_process_id": (
                    last_ok.snapshot.process_id if last_ok and last_ok.snapshot else None
                ),
                "valid_from_seq": state.valid_from_seq,
                "usable_scrapes": len(usable),
                "quarantined": self.issue_is_active(name),
                "issues": [issue.as_dict() for issue in state.issues],
                "last_issue": (
                    state.last_issue.as_dict() if state.last_issue else None
                ),
                "last_issue_age_seconds": (
                    round(now - state.last_issue.at, 4)
                    if state.last_issue
                    else None
                ),
            }
        return result


@asynccontextmanager
async def store_lifespan(store: ScrapeStore):
    await store.start()
    try:
        yield
    finally:
        await store.stop()
