"""End-to-end tests for inconsistent scrapes and restart interleaving.

The verifier is pointed at one healthy sample target and one scripted
"broken" target that can emit mixed process identities, non-monotonic
buckets, a ``+Inf``/``_count`` mismatch, or two individually valid
snapshots whose cumulative increment decreases with the boundary.  The
broken source is also used to prove the quarantine barrier: no sample is
accepted until two new valid same-process scrapes arrive.
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from app.histogram import parse_metrics_text
from app.main import create_app
from app.sample_target import METRIC_NAME
from app.sample_target import create_app as create_target_app

PORT_GOOD = 9401
PORT_BROKEN = 9402
PORT_VERIFIER = 9400
BUCKETS = "0.1,0.5,1"
RUN_ID = "run-clean"


# --------------------------------------------------------------------------- #
# Scripted target
# --------------------------------------------------------------------------- #

def _body(
    *,
    run_id: str = RUN_ID,
    buckets=(10, 30, 40),
    count: int | None = None,
    inf: int | None = None,
    total_sum: float = 5.0,
) -> str:
    count = buckets[-1] if count is None else count
    inf = count if inf is None else inf
    lines = [
        f"# TYPE {METRIC_NAME} histogram",
        f'{METRIC_NAME}_bucket{{le="0.1",run_id="{run_id}"}} {buckets[0]}',
        f'{METRIC_NAME}_bucket{{le="0.5",run_id="{run_id}"}} {buckets[1]}',
        f'{METRIC_NAME}_bucket{{le="1",run_id="{run_id}"}} {buckets[2]}',
        f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{run_id}"}} {inf}',
        f'{METRIC_NAME}_count{{run_id="{run_id}"}} {count}',
        f'{METRIC_NAME}_sum{{run_id="{run_id}"}} {total_sum}',
        "process_start_time_seconds 1.7e9",
    ]
    return "\n".join(lines) + "\n"


CLEAN_BODIES = [
    _body(buckets=(10, 30, 40)),
    _body(buckets=(20, 45, 60), total_sum=8.0),
]


class ScriptBody(BaseModel):
    mode: str = "fixed"
    bodies: list[str] = []


def create_broken_target() -> FastAPI:
    app = FastAPI(title="broken-target")
    app.state.mode = "fixed"
    app.state.bodies = list(CLEAN_BODIES)
    app.state.index = 0

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics() -> PlainTextResponse:
        mode = app.state.mode
        if mode == "decreasing_delta":
            # Every consecutive pair is individually valid but its bucket
            # increment at 0.5 (5) is lower than at 0.1 (10).
            k = app.state.index
            app.state.index += 1
            body = _body(
                buckets=(10 + 10 * k, 30 + 5 * k, 40 + 20 * k),
                total_sum=5.0 + k,
            )
        else:
            bodies = app.state.bodies
            index = min(app.state.index, len(bodies) - 1)
            app.state.index += 1
            body = bodies[index]
        return PlainTextResponse(body, media_type="text/plain; version=0.0.4")

    @app.get("/state")
    def state() -> dict:
        return {"mode": app.state.mode, "index": app.state.index}

    @app.post("/script")
    def script(payload: ScriptBody) -> dict:
        app.state.mode = payload.mode
        if payload.bodies:
            app.state.bodies = list(payload.bodies)
        app.state.index = 0
        return {"ok": True}

    return app


# --------------------------------------------------------------------------- #
# Server helpers
# --------------------------------------------------------------------------- #

class Server:
    def __init__(self, port: int, factory, env: dict[str, str] | None = None):
        self.port = port
        self.factory = factory
        self.env = env or {}

    async def start(self) -> None:
        old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            app = self.factory()
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="error", lifespan="on"
        )
        self.server = uvicorn.Server(config)
        self.task = asyncio.create_task(self.server.serve())
        await self._wait_up()

    async def _wait_up(self, timeout: float = 8.0) -> None:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                try:
                    await client.get(f"http://127.0.0.1:{self.port}/state")
                    return
                except httpx.HTTPError:
                    await asyncio.sleep(0.05)
        raise RuntimeError(f"port {self.port} never came up")

    async def stop(self) -> None:
        self.server.should_exit = True
        await asyncio.wait_for(self.task, timeout=5)


@pytest.fixture
async def rig():
    good = Server(PORT_GOOD, create_target_app,
                  {"INSTANCE_ID": "sample-a", "SAMPLE_BUCKETS": BUCKETS})
    broken = Server(PORT_BROKEN, create_broken_target)
    await asyncio.gather(good.start(), broken.start())

    os.environ["SCRAPE_TARGETS"] = (
        f"sample-a=http://127.0.0.1:{PORT_GOOD}/metrics,"
        f"sample-b=http://127.0.0.1:{PORT_BROKEN}/metrics"
    )
    verifier = Server(PORT_VERIFIER, create_app)
    await verifier.start()
    base = f"http://127.0.0.1:{PORT_VERIFIER}"

    async def script(mode: str, bodies: list[str] | None = None) -> None:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"http://127.0.0.1:{PORT_BROKEN}/script",
                json={"mode": mode, "bodies": bodies or []},
            )

    async def wait_issue(code: str, timeout: float = 6.0) -> dict:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                stats = (await client.get(f"{base}/stats")).json()
                issue = stats["instance_issues"].get("sample-b")
                if issue is not None and issue["code"] == code:
                    return stats
                await asyncio.sleep(0.05)
        raise AssertionError(f"issue {code!r} never surfaced")

    async def wait_recovered(timeout: float = 8.0) -> dict:
        deadline = time.monotonic() + timeout
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                stats = (await client.get(f"{base}/stats")).json()
                sample = stats["current_ready_samples"]["sample-b"]
                if "not_ready_reason" not in sample:
                    return stats
                await asyncio.sleep(0.05)
        raise AssertionError("broken target never recovered")

    yield {"base": base, "script": script, "wait_issue": wait_issue,
           "wait_recovered": wait_recovered, "broken": broken}

    await verifier.stop()
    await asyncio.gather(good.stop(), broken.stop())
    os.environ.pop("SCRAPE_TARGETS", None)


async def _check(base: str, max_wait: float) -> dict:
    async with httpx.AsyncClient(timeout=max_wait + 10) as client:
        resp = await client.get(
            f"{base}/check", params={"threshold": 0.5, "max-wait": max_wait}
        )
        return resp.json()


# --------------------------------------------------------------------------- #
# Inconsistent exposures are named with their instance and reason
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "bad_body, code, needle",
    [
        (
            _body(run_id="run-a").replace(
                'le="+Inf",run_id="run-a"', 'le="+Inf",run_id="run-b"'
            ),
            "mixed_process_identity",
            "多个进程标识",
        ),
        (
            _body(buckets=(50, 40, 60)),
            "non_monotonic_buckets",
            "随边界下降",
        ),
        (
            _body(buckets=(10, 30, 40), count=50, inf=40),
            "inf_mismatch",
            "+Inf",
        ),
    ],
)
async def test_inconsistent_exposure_reported_by_stats(rig, bad_body, code, needle):
    await rig["script"]("fixed", [bad_body])
    stats = await rig["wait_issue"](code)
    info = stats["instances"]["sample-b"]
    assert info["last_issue"]["code"] == code
    assert needle in stats["instance_issues"]["sample-b"]["reason"]
    assert stats["current_ready_samples"]["sample-b"]["not_ready_reason"] == (
        "insufficient_scrapes"
    )

    # The check waits its full original deadline, then reports insufficient
    # evidence naming the instance and why.
    started = time.monotonic()
    result = await _check(rig["base"], max_wait=1.2)
    elapsed = time.monotonic() - started
    assert elapsed >= 1.0
    assert result["decision"] == "inconclusive"
    assert result["timed_out"] is True
    assert "sample-b" in result["missing_instances"]
    assert needle in result["missing_reasons"]["sample-b"]

    # Serving clean, consistent scrapes again clears the issue.
    await rig["script"]("fixed", CLEAN_BODIES)
    stats = await rig["wait_recovered"]()
    assert stats["instance_issues"]["sample-b"]["active"] is False


async def test_contradictory_increment_quarantines_then_recovers(rig):
    await rig["script"]("decreasing_delta")
    stats = await rig["wait_issue"]("cumulative_delta_decreases")
    issue = stats["instance_issues"]["sample-b"]
    assert "累计增量随边界下降" in issue["reason"]
    assert issue["active"] is True
    # The offending snapshots are discarded: no usable scrape remains.
    assert stats["instances"]["sample-b"]["usable_scrapes"] <= 1

    result = await _check(rig["base"], max_wait=1.2)
    assert result["decision"] == "inconclusive"
    assert result["timed_out"] is True
    assert "累计增量随边界下降" in result["missing_reasons"]["sample-b"]

    await rig["script"]("fixed", CLEAN_BODIES)
    stats = await rig["wait_recovered"]()
    assert stats["instance_issues"]["sample-b"]["active"] is False
    result = await _check(rig["base"], max_wait=8)
    assert result["decision"] in ("proceed", "halt", "inconclusive")
    assert result["timed_out"] is False


# --------------------------------------------------------------------------- #
# Live restart interleaving on the real sample target
# --------------------------------------------------------------------------- #

async def test_metrics_stays_atomic_under_restart_and_inject_interleaving():
    server = Server(9411, create_target_app,
                    {"INSTANCE_ID": "sample-a", "SAMPLE_BUCKETS": BUCKETS})
    await server.start()
    base = "http://127.0.0.1:9411"
    parsed = 0

    async with httpx.AsyncClient(timeout=10) as client:
        async def scrape_loop() -> None:
            nonlocal parsed
            for _ in range(60):
                resp = await client.get(f"{base}/metrics")
                snap = parse_metrics_text(resp.text, METRIC_NAME, time.time())
                # parse enforces one process identity, monotone buckets and
                # +Inf == _count for every single response.
                assert snap.count >= 0
                assert snap.buckets[float("inf")] == snap.count
                parsed += 1

        async def write_loop() -> None:
            for index in range(40):
                if index % 7 == 0:
                    resp = await asyncio.wait_for(
                        client.post(f"{base}/restart"), timeout=3
                    )
                    assert resp.json()["ok"] is True
                else:
                    resp = await asyncio.wait_for(
                        client.post(
                            f"{base}/inject",
                            json={"duration_seconds": 0.2, "count": 3},
                        ),
                        timeout=3,
                    )
                    assert resp.json()["ok"] is True

        await asyncio.gather(scrape_loop(), write_loop())

        state = (await client.get(f"{base}/state")).json()
    await server.stop()

    assert parsed == 60
    # Every snapshot copy handed to a scrape was released.
    assert state["outstanding_snapshot_copies"] == 0


async def test_restart_completes_while_metrics_encoding_is_blocked(monkeypatch):
    """Writes finish even while a /metrics body is stuck in encoding."""

    import threading

    import app.sample_target as target_module
    from httpx import ASGITransport, AsyncClient

    app = target_module.create_app()
    state = app.state.target
    state.inject(0.2, 1)

    entered = threading.Event()
    release = threading.Event()
    real_render = target_module.render_metrics

    def blocking_render(view):  # pragma: no cover - exercised below
        entered.set()
        assert release.wait(timeout=5)
        return real_render(view)

    monkeypatch.setattr(target_module, "render_metrics", blocking_render)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://target"
    ) as client:
        scrape = asyncio.create_task(client.get("/metrics"))
        while not entered.is_set():
            await asyncio.sleep(0.01)

        # While the scrape is stuck before its body is produced, both writes
        # complete promptly.
        restart = await asyncio.wait_for(client.post("/restart"), timeout=2)
        inject = await asyncio.wait_for(
            client.post("/inject", json={"duration_seconds": 0.2, "count": 2}),
            timeout=2,
        )
        assert restart.json()["ok"] and inject.json()["ok"]

        release.set()
        response = await asyncio.wait_for(scrape, timeout=5)

    snapshot = parse_metrics_text(response.text, METRIC_NAME, 0.0)
    # The response describes the pre-restart process exactly.
    assert snapshot.count == 1
    assert snapshot.process_id != restart.json()["run_id"]
    state_after = state.state_payload()
    assert state_after["run_id"] == restart.json()["run_id"]
    assert state_after["count"] == 2
    assert state_after["outstanding_snapshot_copies"] == 0
