"""Core logic for the canary rollout SLO check.

This module only contains pure functions / data classes so it can be unit
tested without any HTTP or async machinery.

Prometheus histograms are *cumulative*: every ``_bucket{le="x"}`` counts the
observations whose value is <= x, and ``+Inf`` equals ``_count``.  Between two
scrapes of the same process, the number of requests that landed in a bucket
range is therefore the difference of two cumulative counts.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

INF = float("inf")
# Internal label used by the bundled sample targets to identify a process.
PROCESS_LABEL = "run_id"


class MetricsParseError(ValueError):
    """Raised when exposition text cannot be interpreted as expected."""

    code = "invalid_metrics"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.detail = message


class SnapshotIntegrityError(MetricsParseError):
    """A single scrape is internally inconsistent and must not be used."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class DeltaIntegrityError(MetricsParseError):
    """Two individually valid snapshots do not form a valid increment."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class HistogramSnapshot:
    """One scraped histogram (cumulative bucket counters)."""

    metric_name: str
    buckets: dict[float, int]  # ordered upper bound -> cumulative count
    count: int
    sum_value: float
    process_id: str | None
    scrape_seq: int | None
    scraped_at: float  # wall-clock seconds (time.time())
    # False when the exposition carried no ``_sum`` sample at all.
    sum_present: bool = True

    def __post_init__(self) -> None:
        if INF not in self.buckets:
            object.__setattr__(self, "buckets", {**self.buckets, INF: self.count})


@dataclass
class HistogramDelta:
    """Incremental bucket counts between two snapshots of one process."""

    metric_name: str
    boundaries: list[float]  # finite upper bounds, sorted ascending
    # delta_buckets[i] = requests with value <= boundaries[i] (incremental)
    delta_buckets: list[int]
    delta_inf: int  # total requests in the window
    delta_sum: float
    earlier: HistogramSnapshot
    later: HistogramSnapshot

    @property
    def total_requests(self) -> int:
        return self.delta_inf


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

_LINE_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{.*\})?\s+(?P<value>\S+)\s*$")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"')


def _parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    return {m.group(1): _unescape(m.group(2)) for m in _LABEL_RE.finditer(raw)}


def _unescape(value: str) -> str:
    return re.sub(r"\\(.)", r"\1", value)


def _parse_le(raw: str) -> float:
    if raw in ("+Inf", "-Inf", "Inf"):
        return INF
    return float(raw)


def parse_metrics_text(
    text: str,
    metric_name: str,
    scraped_at: float,
) -> HistogramSnapshot:
    """Parse Prometheus exposition text for one histogram metric.

    Expects ``<metric_name>_bucket{...}``, ``<metric_name>_count`` and
    ``<metric_name>_sum`` samples plus the process identity labels
    ``process_start_time_seconds`` / ``sample_run_id``.

    Every scrape is validated as a self-consistent snapshot: all histogram
    samples must carry one and the same process identity (mixed identities
    inside one exposition are rejected), and the cumulative bucket counts
    must be monotone in the boundary, ``+Inf`` must equal ``_count``, the
    finite buckets must not exceed ``_count`` and ``_sum`` must be plausible.
    """

    buckets: dict[float, int] = {}
    count: int | None = None
    sum_value: float | None = None
    process_start: float | None = None
    scrape_seq: int | None = None
    identities: set[str] = set()

    def observe_identity(labels: dict[str, str]) -> None:
        if PROCESS_LABEL in labels:
            identities.add(labels[PROCESS_LABEL])
        elif "instance" in labels:
            identities.add(labels["instance"])

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _LINE_RE.match(line)
        if not match:
            continue
        name = match.group("name")
        labels = _parse_labels(match.group("labels"))
        value_raw = match.group("value")

        if name == "process_start_time_seconds":
            process_start = float(value_raw)
            continue
        if name == "sample_scrape_seq":
            scrape_seq = int(float(value_raw))
            continue

        if name == f"{metric_name}_bucket":
            if "le" not in labels:
                continue
            boundary = _parse_le(labels["le"])
            if boundary in buckets:
                raise SnapshotIntegrityError(
                    "duplicate_bucket",
                    f"bucket le={labels['le']} appears more than once in one scrape",
                )
            buckets[boundary] = int(float(value_raw))
            observe_identity(labels)
        elif name == f"{metric_name}_count":
            count = int(float(value_raw))
            observe_identity(labels)
        elif name == f"{metric_name}_sum":
            sum_value = float(value_raw)
            observe_identity(labels)

    if not buckets:
        raise MetricsParseError(f"no buckets found for metric {metric_name!r}")

    if len(identities) > 1:
        raise SnapshotIntegrityError(
            "mixed_process_identity",
            "one scrape contains samples from multiple process identities: "
            + ", ".join(sorted(identities)),
        )

    if count is None:
        count = buckets.get(INF)
    if count is None:
        raise MetricsParseError(f"no {metric_name}_count sample and no +Inf bucket")

    # Process identity: prefer the run_id label on histogram samples; fall
    # back to the process_start_time_seconds gauge when the label is absent.
    process_id = next(iter(identities), None)
    if process_id is None and process_start is not None:
        process_id = f"start-{process_start:f}"

    if process_id is None:
        raise SnapshotIntegrityError(
            "missing_process_identity",
            "scrape exposes neither a run_id/instance label nor "
            "process_start_time_seconds",
        )

    ordered = dict(sorted(buckets.items(), key=lambda item: item[0]))
    snapshot = HistogramSnapshot(
        metric_name=metric_name,
        buckets=ordered,
        count=count,
        sum_value=0.0 if sum_value is None else sum_value,
        process_id=process_id,
        scrape_seq=scrape_seq,
        scraped_at=scraped_at,
        sum_present=sum_value is not None,
    )
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot: HistogramSnapshot) -> None:
    """Reject a scrape whose bucket series cannot describe one instant.

    The checks catch a scrape that observed the histogram while it was being
    mutated (e.g. the process restarted mid-response or a writer updated the
    buckets between the ``+Inf`` and the finite counters), which would
    otherwise corrupt every increment derived from it.
    """

    if snapshot.process_id is None or not str(snapshot.process_id).strip():
        raise SnapshotIntegrityError(
            "missing_process_identity", "scrape carries no process identity"
        )

    finite = [boundary for boundary in snapshot.buckets if math.isfinite(boundary)]
    if not finite:
        raise SnapshotIntegrityError(
            "missing_finite_buckets", "scrape exposes no finite bucket boundaries"
        )

    previous = None
    for boundary in finite:
        value = snapshot.buckets[boundary]
        if value < 0:
            raise SnapshotIntegrityError(
                "negative_bucket", f"bucket le={boundary:g} has negative count {value}"
            )
        if previous is not None and value < previous[1]:
            raise SnapshotIntegrityError(
                "non_monotonic_buckets",
                f"bucket le={boundary:g} ({value}) is lower than the previous "
                f"boundary le={previous[0]:g} ({previous[1]})",
            )
        if value > snapshot.count:
            raise SnapshotIntegrityError(
                "bucket_exceeds_total",
                f"bucket le={boundary:g} ({value}) exceeds _count "
                f"({snapshot.count})",
            )
        previous = (boundary, value)

    inf_value = snapshot.buckets.get(INF)
    if inf_value is not None and inf_value != snapshot.count:
        raise SnapshotIntegrityError(
            "inf_mismatch",
            f"+Inf bucket ({inf_value}) does not equal _count "
            f"({snapshot.count})",
        )
    if previous is not None and previous[1] > snapshot.count:
        raise SnapshotIntegrityError(
            "bucket_exceeds_total",
            f"largest finite bucket ({previous[1]}) exceeds _count "
            f"({snapshot.count})",
        )
    if snapshot.count < 0 or snapshot.sum_value < 0:
        raise SnapshotIntegrityError(
            "negative_total", "histogram _count/_sum is negative"
        )


# --------------------------------------------------------------------------- #
# Bucket increments
# --------------------------------------------------------------------------- #

def compute_delta(earlier: HistogramSnapshot, later: HistogramSnapshot) -> HistogramDelta:
    """Incremental counts between two cumulative snapshots of one process.

    Both snapshots must be internally valid and belong to the same process,
    with identical bucket layouts.  The resulting increment must itself be a
    valid cumulative series: every cumulative increment is non-negative,
    non-decreasing with the boundary, and the largest finite increment never
    exceeds ``_count``.  A violation means the two "snapshots" did not
    describe one monotone process lifetime (tearing, restart, mixed data).
    """

    if earlier.metric_name != later.metric_name:
        raise DeltaIntegrityError(
            "different_metric", "snapshots describe different metrics"
        )
    if earlier.process_id != later.process_id:
        raise DeltaIntegrityError(
            "process_restarted",
            f"snapshots come from different process identities "
            f"({earlier.process_id!r} vs {later.process_id!r})",
        )
    validate_snapshot(earlier)
    validate_snapshot(later)

    if list(earlier.buckets.keys()) != list(later.buckets.keys()):
        raise DeltaIntegrityError(
            "bucket_layout_changed",
            "bucket boundaries differ between the two scrapes; "
            "they must come from the same process",
        )

    boundaries = [b for b in later.buckets if math.isfinite(b)]
    # Difference of two cumulative series at the same bound = cumulative
    # count of window requests whose value is <= that boundary.
    delta_buckets: list[int] = []
    previous: tuple[float, int] | None = None
    for boundary in boundaries:
        diff = later.buckets[boundary] - earlier.buckets[boundary]
        if diff < 0:
            raise DeltaIntegrityError(
                "counter_reset",
                f"cumulative bucket le={boundary:g} fell by {-diff} "
                f"({earlier.buckets[boundary]} -> {later.buckets[boundary]})",
            )
        if previous is not None and diff < previous[1]:
            raise DeltaIntegrityError(
                "cumulative_delta_decreases",
                f"cumulative increment at le={boundary:g} ({diff}) is lower "
                f"than at the smaller boundary le={previous[0]:g} "
                f"({previous[1]})",
            )
        previous = (boundary, diff)
        delta_buckets.append(diff)

    delta_inf = later.count - earlier.count
    if delta_inf < 0:
        raise DeltaIntegrityError(
            "counter_reset",
            f"histogram _count fell by {-delta_inf} "
            f"({earlier.count} -> {later.count})",
        )
    # Only compare ``_sum`` when both exposures actually carried it; a
    # missing sample is treated as zero elsewhere and must not masquerade
    # as a counter reset.
    if (
        earlier.sum_present
        and later.sum_present
        and later.sum_value < earlier.sum_value - 1e-9 * max(1.0, abs(earlier.sum_value))
    ):
        raise DeltaIntegrityError(
            "counter_reset",
            f"histogram _sum fell ({earlier.sum_value} -> {later.sum_value})",
        )
    if previous is not None and previous[1] > delta_inf:
        raise DeltaIntegrityError(
            "delta_exceeds_total",
            f"largest finite cumulative increment ({previous[1]}) exceeds the "
            f"_count increment ({delta_inf})",
        )
    delta_sum = (
        max(0.0, later.sum_value - earlier.sum_value)
        if earlier.sum_present and later.sum_present
        else 0.0
    )

    return HistogramDelta(
        metric_name=later.metric_name,
        boundaries=boundaries,
        delta_buckets=delta_buckets,
        delta_inf=delta_inf,
        delta_sum=delta_sum,
        earlier=earlier,
        later=later,
    )


# --------------------------------------------------------------------------- #
# SLO bounds
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Interval:
    """Closed-ish interval [lower, upper] of request counts/ratios."""

    lower: float
    upper: float

    def __post_init__(self) -> None:
        if self.lower < -1e-12 or self.upper < self.lower - 1e-12:
            raise ValueError(f"invalid interval: {self.lower}..{self.upper}")


def compliant_count_bounds(delta: HistogramDelta, threshold_s: float) -> Interval:
    """Bounds on the number of requests completed within ``threshold_s``.

    Prometheus only tells us which *bucket* each request fell into, so when
    the threshold cuts through a bucket the exact answer is unknown and an
    uncertainty range is returned.

    Given cumulative bucket counts ``C(b)`` (# requests <= b):

    * requests definitely <= threshold:  C(largest boundary <= threshold)
    * requests possibly   <= threshold:  C(smallest boundary >= threshold)
      (a threshold equal to a boundary makes that bucket fully known, so the
      two coincide)
    """

    if threshold_s < 0:
        raise ValueError("threshold must be non-negative")

    # delta_buckets are per-upper-bound *cumulative* counts of the window;
    # the +Inf equivalent is delta_inf.
    definitely = 0
    possibly: int | None = None

    for boundary, cumulative in zip(delta.boundaries, delta.delta_buckets):
        if boundary < threshold_s:
            definitely = cumulative
        elif boundary == threshold_s:
            # Threshold on a boundary: everything up to it is known.
            definitely = cumulative
            possibly = cumulative
        else:  # boundary > threshold_s: first such bucket straddles it
            if possibly is None:
                possibly = cumulative

    if possibly is None:
        # Threshold above the largest finite boundary: the tail bucket is
        # (largest_bound, +Inf), every request there may or may not qualify.
        possibly = delta.delta_inf

    return Interval(lower=definitely, upper=possibly)


@dataclass
class CombinedResult:
    total_requests: int
    compliant_lower: int
    compliant_upper: int
    lower_ratio: float
    upper_ratio: float
    per_instance: dict[str, Interval] = field(default_factory=dict)

    @property
    def is_zero_traffic(self) -> bool:
        return self.total_requests == 0


def combine_instances(
    deltas: dict[str, HistogramDelta], threshold_s: float
) -> CombinedResult:
    """Sum SLO bounds across instances with potentially different buckets."""

    per_instance: dict[str, Interval] = {}
    total = 0
    lower = 0
    upper = 0
    for instance, delta in deltas.items():
        bounds = compliant_count_bounds(delta, threshold_s)
        per_instance[instance] = bounds
        total += delta.total_requests
        lower += int(bounds.lower)
        upper += int(bounds.upper)

    if total == 0:
        lower_ratio = upper_ratio = 0.0
    else:
        lower_ratio = lower / total
        upper_ratio = upper / total

    return CombinedResult(
        total_requests=total,
        compliant_lower=lower,
        compliant_upper=upper,
        lower_ratio=lower_ratio,
        upper_ratio=upper_ratio,
        per_instance=per_instance,
    )
