"""Tests for :mod:`tickforge.feed.rest`.

A local ``aiohttp`` server bound to ``127.0.0.1:0`` serves every response; no
public network access is performed.  The suite covers successful snapshots
(raw string preservation, query parameters, timestamps), retry policy,
non-retryable errors, timeouts, payload validation and client lifecycle.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from aiohttp import web

from tickforge.config import FeedConfig
from tickforge.feed.rest import SnapshotClient, SnapshotData, SnapshotError

PAYLOAD: dict[str, Any] = {
    "lastUpdateId": 1_027_024,
    "bids": [["4.0", "431.00000000"], ["3.90000000", "0.50000000"]],
    "asks": [["4.00000200", "12.0"]],
}

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


def make_cfg(rest_url: str, **overrides: Any) -> FeedConfig:
    values: dict[str, Any] = dict(
        rest_url=rest_url,
        symbols=["BTCUSDT"],
        snapshot_limit=1000,
        snapshot_timeout_s=2.0,
        snapshot_retries=2,
        snapshot_retry_backoff_s=0.0,
    )
    values.update(overrides)
    return FeedConfig(**values)


class FakeMetrics:
    def __init__(self) -> None:
        self.increments: list[tuple[str, dict[str, str], float]] = []
        self.observations: list[tuple[str, int, dict[str, str]]] = []

    def inc(
        self, name: str, labels: dict[str, str] | None = None, value: float = 1.0
    ) -> None:
        self.increments.append((name, dict(labels or {}), float(value)))

    def observe_ns(
        self, name: str, value: int, labels: dict[str, str] | None = None
    ) -> None:
        self.observations.append((name, int(value), dict(labels or {})))

    def set_gauge(
        self, name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:  # pragma: no cover - not used by the REST client
        pass

    def request_statuses(self) -> list[str]:
        return [
            labels["status"]
            for name, labels, _ in self.increments
            if name == "tickforge_snapshot_requests_total"
        ]


class BrokenMetrics:
    def inc(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")

    def observe_ns(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")

    def set_gauge(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")


@asynccontextmanager
async def running_rest(handler: Handler) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_get("/api/v3/depth", handler)
    # A short shutdown timeout keeps the timeout test quick: the still-sleeping
    # mock handler is cancelled during cleanup.
    runner = web.AppRunner(app, access_log=None, shutdown_timeout=0.2)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None and site._server.sockets
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_fetch_success_preserves_raw_strings_and_sends_query() -> None:
    requests: list[web.Request] = []
    server_received_ns: list[int] = []

    async def handler(request: web.Request) -> web.Response:
        server_received_ns.append(time.perf_counter_ns())
        requests.append(request)
        return web.json_response(PAYLOAD)

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        # Trailing slash must not produce a double slash in the URL.
        client = SnapshotClient(make_cfg(url + "/"), metrics=metrics)
        try:
            data = await client.fetch("btcusdt")
        finally:
            await client.close()

    assert len(requests) == 1
    request = requests[0]
    assert request.path == "/api/v3/depth"
    assert request.query["symbol"] == "BTCUSDT"
    assert request.query["limit"] == "1000"
    assert request.headers["User-Agent"].startswith("TickForge/")
    assert request.headers["Accept"] == "application/json"

    assert isinstance(data, SnapshotData)
    assert data.symbol == "BTCUSDT"
    assert data.last_update_id == 1_027_024
    # Raw exchange strings are preserved, not converted through float.
    assert data.bids == [["4.0", "431.00000000"], ["3.90000000", "0.50000000"]]
    assert data.asks == [["4.00000200", "12.0"]]
    assert isinstance(data.bids, list) and isinstance(data.bids[0], list)
    assert all(isinstance(value, str) for level in data.bids for value in level)

    now_mono = time.perf_counter_ns()
    now_wall = time.time_ns()
    # requested_mono_ns is sampled before the request hits the wire ...
    assert 0 < data.requested_mono_ns <= server_received_ns[0]
    assert data.requested_mono_ns <= data.recv_mono_ns <= now_mono
    assert 0 < data.recv_wall_ns <= now_wall
    assert now_wall - data.recv_wall_ns < 5_000_000_000

    assert metrics.request_statuses() == ["success"]
    assert len(metrics.observations) == 1
    name, value, labels = metrics.observations[0]
    assert name == "tickforge_snapshot_latency_ns"
    assert value >= 0 and labels == {}
    bytes_total = sum(
        amount
        for metric, _, amount in metrics.increments
        if metric == "tickforge_snapshot_bytes_total"
    )
    assert bytes_total > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_status", [500, 502, 503, 429])
async def test_retries_transient_status_then_succeeds(failure_status: int) -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=failure_status, text="transient")
        return web.json_response(PAYLOAD)

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        cfg = make_cfg(url, snapshot_retries=3)
        client = SnapshotClient(cfg, metrics=metrics)
        try:
            data = await client.fetch("BTCUSDT")
        finally:
            await client.close()

    assert calls == 2
    assert data.last_update_id == PAYLOAD["lastUpdateId"]
    assert metrics.request_statuses() == [f"http_{failure_status}", "success"]


@pytest.mark.asyncio
@pytest.mark.parametrize("client_status", [400, 404, 418])
async def test_client_errors_are_not_retried(client_status: int) -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        return web.Response(status=client_status, text="nope")

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url, snapshot_retries=4), metrics=metrics)
        try:
            with pytest.raises(SnapshotError) as excinfo:
                await client.fetch("BTCUSDT")
        finally:
            await client.close()

    assert calls == 1
    assert f"HTTP {client_status}" in str(excinfo.value)
    assert metrics.request_statuses() == [f"http_{client_status}"]
    assert metrics.observations == []


@pytest.mark.asyncio
async def test_network_error_is_retried_then_reported() -> None:
    accepts = 0

    async def reject(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepts
        accepts += 1
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    tcp_server = await asyncio.start_server(reject, "127.0.0.1", 0)
    port = tcp_server.sockets[0].getsockname()[1]
    metrics = FakeMetrics()
    cfg = make_cfg(
        f"http://127.0.0.1:{port}", snapshot_retries=1, snapshot_timeout_s=1.0
    )
    client = SnapshotClient(cfg, metrics=metrics)
    try:
        with pytest.raises(SnapshotError) as excinfo:
            await client.fetch("BTCUSDT")
    finally:
        await client.close()
        tcp_server.close()
        await tcp_server.wait_closed()

    assert accepts >= 2
    assert "failed" in str(excinfo.value)
    assert metrics.request_statuses() == ["network_error", "network_error"]
    assert metrics.observations == []


@pytest.mark.asyncio
async def test_failure_after_exhausting_retries() -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        return web.Response(status=503, text="still down")

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url, snapshot_retries=2), metrics=metrics)
        try:
            with pytest.raises(SnapshotError) as excinfo:
                await client.fetch("BTCUSDT")
        finally:
            await client.close()

    # snapshot_retries retries follow the initial attempt.
    assert calls == 3
    assert "HTTP 503" in str(excinfo.value)
    assert metrics.request_statuses() == ["http_503"] * 3
    assert metrics.observations == []


@pytest.mark.asyncio
async def test_timeout_is_retried_and_reported() -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.4)
        return web.json_response(PAYLOAD)

    metrics = FakeMetrics()
    started = time.monotonic()
    async with running_rest(handler) as url:
        cfg = make_cfg(
            url, snapshot_timeout_s=0.05, snapshot_retries=1, snapshot_retry_backoff_s=0.0
        )
        client = SnapshotClient(cfg, metrics=metrics)
        try:
            with pytest.raises(SnapshotError) as excinfo:
                await client.fetch("BTCUSDT")
        finally:
            await client.close()

    elapsed = time.monotonic() - started
    assert calls == 2
    assert "timed out" in str(excinfo.value)
    assert metrics.request_statuses() == ["timeout", "timeout"]
    assert elapsed < 2.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(
            {"bids": [["1.0", "1.0"]], "asks": [["2.0", "1.0"]]},
            id="missing-lastUpdateId",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": [], "asks": [["2.0", "1.0"]]},
            id="empty-bids",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": [["1.0", "1.0"]], "asks": []},
            id="empty-asks",
        ),
        pytest.param(
            {"lastUpdateId": True, "bids": [["1.0", "1.0"]], "asks": [["2.0", "1.0"]]},
            id="bool-lastUpdateId",
        ),
        pytest.param(
            {"lastUpdateId": "abc", "bids": [["1.0", "1.0"]], "asks": [["2.0", "1.0"]]},
            id="non-numeric-lastUpdateId",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": "not-a-list", "asks": [["2.0", "1.0"]]},
            id="bids-not-a-list",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": [["1.0"]], "asks": [["2.0", "1.0"]]},
            id="short-level",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": [[1.0, "1.0"]], "asks": [["2.0", "1.0"]]},
            id="numeric-price",
        ),
        pytest.param(
            {"lastUpdateId": 1, "bids": [["nope", "1.0"]], "asks": [["2.0", "1.0"]]},
            id="invalid-decimal",
        ),
        pytest.param([], id="top-level-array"),
    ],
)
async def test_payload_validation_errors_are_not_retried(payload: Any) -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        return web.json_response(payload)

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url, snapshot_retries=3), metrics=metrics)
        try:
            with pytest.raises(SnapshotError):
                await client.fetch("BTCUSDT")
        finally:
            await client.close()

    assert calls == 1
    assert metrics.request_statuses() == ["invalid"]


@pytest.mark.asyncio
async def test_invalid_json_is_not_retried() -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        return web.Response(body=b"{not-json", content_type="application/json")

    metrics = FakeMetrics()
    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url, snapshot_retries=3), metrics=metrics)
        try:
            with pytest.raises(SnapshotError) as excinfo:
                await client.fetch("BTCUSDT")
        finally:
            await client.close()

    assert calls == 1
    assert "JSON" in str(excinfo.value)
    assert metrics.request_statuses() == ["invalid"]


@pytest.mark.asyncio
async def test_lowercase_symbol_is_uppercased_and_string_update_id_accepted() -> None:
    seen_symbols: list[str] = []
    payload = {"lastUpdateId": "42", "bids": [["1.0", "1.0"]], "asks": [["2.0", "1.0"]]}

    async def handler(request: web.Request) -> web.Response:
        seen_symbols.append(request.query["symbol"])
        return web.json_response(payload)

    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url))
        try:
            data = await client.fetch("ethusdt")
        finally:
            await client.close()

    assert seen_symbols == ["ETHUSDT"]
    assert data.symbol == "ETHUSDT"
    assert data.last_update_id == 42


@pytest.mark.asyncio
async def test_fetch_reuses_one_session() -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        return web.json_response(PAYLOAD)

    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url))
        try:
            session = client._session
            assert session is None  # lazily created
            await client.fetch("BTCUSDT")
            first_session = client._session
            await client.fetch("BTCUSDT")
            assert client._session is first_session
        finally:
            await client.close()
    assert calls == 2


@pytest.mark.asyncio
async def test_close_is_idempotent_and_fetch_after_close_fails() -> None:
    client = SnapshotClient(make_cfg("http://127.0.0.1:1"))
    await client.close()
    await client.close()
    with pytest.raises(SnapshotError):
        await client.fetch("BTCUSDT")


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_symbol", ["", "   ", None, 123])
async def test_invalid_symbol_rejected(bad_symbol: Any) -> None:
    client = SnapshotClient(make_cfg("http://127.0.0.1:1"))
    try:
        with pytest.raises(SnapshotError):
            await client.fetch(bad_symbol)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_metrics_failures_are_swallowed() -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(PAYLOAD)

    async with running_rest(handler) as url:
        client = SnapshotClient(make_cfg(url), metrics=BrokenMetrics())
        try:
            data = await client.fetch("BTCUSDT")
        finally:
            await client.close()
    assert data.last_update_id == PAYLOAD["lastUpdateId"]


@pytest.mark.asyncio
async def test_retry_backoff_sleeps_between_attempts() -> None:
    calls = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return web.Response(status=500, text="boom")
        return web.json_response(PAYLOAD)

    async with running_rest(handler) as url:
        cfg = make_cfg(url, snapshot_retries=1, snapshot_retry_backoff_s=0.05)
        client = SnapshotClient(cfg)
        started = time.monotonic()
        try:
            await client.fetch("BTCUSDT")
        finally:
            await client.close()
    elapsed = time.monotonic() - started

    assert calls == 2
    assert elapsed >= 0.04  # the configured backoff was honoured
