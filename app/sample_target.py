"""Local sample target exposing a Prometheus latency histogram.

Two instances of this app are started with different ``SAMPLE_BUCKETS``
values so the verifier has to cope with differing bucket layouts.

Runtime controls (used by tests and demos):

* ``POST /inject``  -- add one or more request observations
                       body: ``{"duration_seconds": 0.23, "count": 1}``
* ``POST /restart`` -- simulate a process restart: new run id, new process
                       start time, all counters reset to zero
* ``GET  /state``   -- inspect the in-memory counters

Consistency model
-----------------
Writes (inject / restart) mutate the live counters in place under a lock.
``GET /metrics`` takes **one** immutable snapshot of the whole histogram
(run id, process start, every bucket, ``_count``, ``_sum`` and the scrape
sequence) while holding that lock for a few microseconds, then releases it
*before* the body is encoded.  Consequently:

* every response describes exactly one process at exactly one instant -- a
  restart or an injection can never tear the exposition in half;
* a slow / stuck client only retains the one snapshot copy it is being
  served, so the number of retained old bucket copies is bounded by the
  number of in-flight scrapes, no matter how many writes arrive;
* writers never wait for response encoding, so inject / restart complete
  even while scrapes are blocked on their bodies.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from dataclasses import dataclass

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

METRIC_NAME = "http_request_duration_seconds"
DEFAULT_BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
DEFAULT_BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class InjectBody(BaseModel):
    duration_seconds: float = Field(..., ge=0)
    count: int = Field(1, ge=1, le=100_000)


@dataclass(frozen=True)
class HistogramView:
    """One immutable, internally consistent view of the target process."""

    run_id: str
    process_start: float
    boundaries: tuple[float, ...]
    bucket_counts: dict[float, int]
    total_count: int
    total_sum: float
    scrape_seq: int
    restart_count: int


class TargetState:
    def __init__(self, instance_id: str, boundaries: list[float]) -> None:
        self.instance_id = instance_id
        self.boundaries = boundaries
        self._lock = threading.Lock()
        # Number of snapshot copies currently held by in-flight scrapes.
        self._outstanding_snapshots = 0
        self.reset()

    def reset(self) -> None:
        """Restart the (simulated) process atomically."""
        with self._lock:
            self.run_id = uuid.uuid4().hex[:12]
            self.process_start = time.time()
            # cumulative counts per finite boundary + +Inf
            self.bucket_counts: dict[float, int] = {b: 0 for b in self.boundaries}
            self.total_count = 0
            self.total_sum = 0.0
            self.scrape_seq = 0
            self.restart_count = getattr(self, "restart_count", 0) + 1

    def inject(self, duration: float, count: int) -> int:
        with self._lock:
            for boundary in self.boundaries:
                if duration <= boundary:
                    self.bucket_counts[boundary] += count
            self.total_count += count
            self.total_sum += duration * count
            return self.total_count

    # -- read side ----------------------------------------------------- #

    def take_snapshot(self) -> HistogramView:
        """Atomically copy the whole histogram; also count it as in flight.

        The caller must call :meth:`release_snapshot` exactly once when it is
        done with the view.  Only this copy (not every write) is retained, so
        old bucket copies never outnumber in-flight scrapes.
        """
        with self._lock:
            self.scrape_seq += 1
            self._outstanding_snapshots += 1
            return HistogramView(
                run_id=self.run_id,
                process_start=self.process_start,
                boundaries=tuple(self.boundaries),
                bucket_counts=dict(self.bucket_counts),
                total_count=self.total_count,
                total_sum=self.total_sum,
                scrape_seq=self.scrape_seq,
                restart_count=self.restart_count,
            )

    def release_snapshot(self) -> None:
        with self._lock:
            if self._outstanding_snapshots > 0:
                self._outstanding_snapshots -= 1

    def state_payload(self) -> dict:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "run_id": self.run_id,
                "process_start_time_seconds": self.process_start,
                "boundaries": list(self.boundaries),
                "bucket_counts": dict(self.bucket_counts),
                "count": self.total_count,
                "sum": self.total_sum,
                "restart_count": self.restart_count,
                "scrape_seq": self.scrape_seq,
                "outstanding_snapshot_copies": self._outstanding_snapshots,
            }


def render_metrics(view: HistogramView) -> str:
    """Render one exposition body from a single immutable view."""

    lines = [
        f"# HELP {METRIC_NAME} request latency in seconds",
        f"# TYPE {METRIC_NAME} histogram",
    ]
    for boundary in view.boundaries:
        lines.append(
            f'{METRIC_NAME}_bucket{{le="{boundary:g}",run_id="{view.run_id}"}} '
            f"{view.bucket_counts[boundary]}"
        )
    lines.append(
        f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{view.run_id}"}} '
        f"{view.total_count}"
    )
    lines.append(
        f'{METRIC_NAME}_count{{run_id="{view.run_id}"}} {view.total_count}'
    )
    lines.append(
        f'{METRIC_NAME}_sum{{run_id="{view.run_id}"}} {view.total_sum}'
    )
    lines.extend(
        [
            "# HELP process_start_time_seconds start time of the process since unix epoch",
            "# TYPE process_start_time_seconds gauge",
            f"process_start_time_seconds {view.process_start:.6f}",
            "# HELP sample_scrape_seq number of scrapes served since process start",
            "# TYPE sample_scrape_seq gauge",
            f"sample_scrape_seq {view.scrape_seq}",
        ]
    )
    return "\n".join(lines) + "\n"


def _read_buckets(instance_id: str) -> list[float]:
    raw = os.environ.get("SAMPLE_BUCKETS")
    if raw is None:
        raw = DEFAULT_BUCKETS_A if instance_id.endswith("a") else DEFAULT_BUCKETS_B
    values = [float(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise ValueError("SAMPLE_BUCKETS must contain at least one boundary")
    return sorted(values)


def create_app() -> FastAPI:
    instance_id = os.environ.get("INSTANCE_ID", "sample-a")
    state = TargetState(instance_id, _read_buckets(instance_id))
    app = FastAPI(title=f"sample-target-{instance_id}", version="1.0.0")
    app.state.target = state

    @app.post("/inject")
    def inject(body: InjectBody) -> dict:
        total = state.inject(body.duration_seconds, body.count)
        return {"ok": True, "total_count": total}

    @app.post("/restart")
    def restart() -> dict:
        state.reset()
        return {"ok": True, "run_id": state.run_id}

    @app.get("/state")
    def get_state() -> dict:
        return state.state_payload()

    # Plain-text exposition, returned with the Prometheus content type.
    @app.get("/metrics", response_class=PlainTextResponse)
    def render_metrics_endpoint() -> PlainTextResponse:
        # The view is an immutable deep copy of the whole histogram; the
        # lock is released before the body is rendered/encoded, so a writer
        # (inject / restart) never waits for a slow scrape response.
        view = state.take_snapshot()
        try:
            body = render_metrics(view)
        finally:
            state.release_snapshot()
        return PlainTextResponse(body, media_type="text/plain; version=0.0.4")

    return app


app = create_app()
