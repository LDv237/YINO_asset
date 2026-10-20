"""Tests for the aiohttp observability server (real sockets on localhost)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp
import pytest

from tickforge.metrics import MetricsRegistry, NullMetrics
from tickforge.server import METRICS_CONTENT_TYPE, HttpServer


def run(coro: Any) -> Any:
    """Run one scenario in a fresh event loop (no pytest-asyncio dependency)."""

    return asyncio.run(coro)


async def _request(
    port: int, path: str
) -> tuple[int, str, str]:
    """Return ``(status, content_type, body)`` for a localhost GET."""

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(trust_env=False, timeout=timeout) as session:
        url = f"http://127.0.0.1:{port}{path}"
        async with session.get(url) as response:
            return (
                response.status,
                response.headers.get("Content-Type", ""),
                await response.text(),
            )


def test_health_status_metrics_and_root_endpoints() -> None:
    async def scenario() -> None:
        metrics = MetricsRegistry()
        metrics.inc("tickforge_ws_messages_total", None, 3)
        metrics.inc("tickforge_ws_messages_total", {"symbol": "BTCUSDT"}, 2)

        health_state = {"ok": True}

        def health_fn() -> dict:
            return {"ok": health_state["ok"], "detail": "test"}

        def status_fn() -> dict:
            return {"symbols": ["BTCUSDT"], "wal_records": 17}

        server = HttpServer(
            metrics, health_fn=health_fn, status_fn=status_fn, port=0
        )
        assert server.port == 0
        assert server.started is False
        await server.start()
        try:
            assert server.started is True
            port = server.port
            assert port > 0

            status, content_type, body = await _request(port, "/healthz")
            assert status == 200
            assert content_type.startswith("application/json")
            assert json.loads(body) == {"ok": True, "detail": "test"}

            status, content_type, body = await _request(port, "/readyz")
            assert status == 200
            assert json.loads(body)["ok"] is True

            status, content_type, body = await _request(port, "/status")
            assert status == 200
            assert content_type.startswith("application/json")
            assert json.loads(body) == {"symbols": ["BTCUSDT"], "wal_records": 17}

            status, content_type, body = await _request(port, "/metrics")
            assert status == 200
            assert content_type == METRICS_CONTENT_TYPE
            assert "tickforge_ws_messages_total 3.0" in body
            assert (
                'tickforge_ws_messages_total{symbol="BTCUSDT"} 2.0' in body
            )

            status, content_type, body = await _request(port, "/")
            assert status == 200
            assert content_type.startswith("text/plain")
            assert "/healthz" in body and "/metrics" in body

            status, _, _ = await _request(port, "/not-a-route")
            assert status == 404

            # The same health callback drives readiness.
            health_state["ok"] = False
            for path in ("/healthz", "/readyz"):
                status, _, body = await _request(port, path)
                assert status == 503
                assert json.loads(body)["ok"] is False
        finally:
            await server.stop()

    run(scenario())


def test_start_stop_are_idempotent_and_restartable() -> None:
    async def scenario() -> None:
        server = HttpServer(
            NullMetrics(),
            health_fn=lambda: {"ok": True},
            status_fn=dict,
            port=0,
        )
        # stop() before start() is a harmless no-op.
        await server.stop()
        assert server.started is False

        await server.start()
        first_port = server.port
        assert first_port > 0
        # Calling start() again must not rebind or change the port.
        await server.start()
        assert server.port == first_port
        assert server.started is True

        status, _, body = await _request(first_port, "/metrics")
        assert status == 200
        assert body == ""

        await server.stop()
        assert server.started is False
        await server.stop()  # second stop is safe

        # A stopped server can be started again (a fresh ephemeral port).
        await server.start()
        second_port = server.port
        assert second_port > 0
        status, _, _ = await _request(second_port, "/healthz")
        assert status == 200
        await server.stop()

    run(scenario())


def test_failing_callbacks_degrade_to_503_and_500() -> None:
    async def scenario() -> None:
        def boom() -> dict:
            raise RuntimeError("boom")

        server = HttpServer(
            NullMetrics(), health_fn=boom, status_fn=boom, port=0
        )
        await server.start()
        try:
            port = server.port
            for path in ("/healthz", "/readyz"):
                status, content_type, body = await _request(port, path)
                assert status == 503
                assert content_type.startswith("application/json")
                assert json.loads(body)["ok"] is False
            status, _, body = await _request(port, "/status")
            assert status == 500
            assert "error" in json.loads(body)
        finally:
            await server.stop()

        # Non-mapping health payload => not ready.  Unserializable payloads
        # must also fail closed rather than raise through aiohttp.
        def unserializable_status() -> dict:
            return {"bad": object()}

        server = HttpServer(
            NullMetrics(),
            health_fn=lambda: None,
            status_fn=unserializable_status,
            port=0,
        )
        await server.start()
        try:
            port = server.port
            status, _, body = await _request(port, "/healthz")
            assert status == 503
            assert json.loads(body)["ok"] is False
            status, _, body = await _request(port, "/status")
            assert status == 500
            assert "error" in json.loads(body)
        finally:
            await server.stop()

        server = HttpServer(
            NullMetrics(),
            health_fn=lambda: {"ok": True, "bad": object()},
            status_fn=dict,
            port=0,
        )
        await server.start()
        try:
            status, _, body = await _request(server.port, "/healthz")
            assert status == 503
            assert json.loads(body)["ok"] is False
        finally:
            await server.stop()

    run(scenario())


def test_port_already_in_use_raises_and_leaves_server_stopped() -> None:
    async def scenario() -> None:
        first = HttpServer(
            NullMetrics(),
            health_fn=lambda: {"ok": True},
            status_fn=dict,
            port=0,
        )
        await first.start()
        second = HttpServer(
            NullMetrics(),
            health_fn=lambda: {"ok": True},
            status_fn=dict,
            port=first.port,  # deliberate collision with the running server
        )
        try:
            with pytest.raises(OSError):
                await second.start()
            assert second.started is False

            # After the first server releases the port the second one must be
            # able to start: the failed attempt fully cleaned up its runner.
            await first.stop()
            await second.start()
            status, _, _ = await _request(second.port, "/healthz")
            assert status == 200
        finally:
            await first.stop()
            await second.stop()

    run(scenario())


def test_metrics_endpoint_reflects_live_updates() -> None:
    async def scenario() -> None:
        metrics = MetricsRegistry()
        server = HttpServer(
            metrics,
            health_fn=lambda: {"ok": True},
            status_fn=lambda: metrics.snapshot(),
            port=0,
        )
        await server.start()
        try:
            port = server.port
            metrics.inc("tickforge_snapshot_requests_total", {"status": "ok"})
            metrics.observe_ns("tickforge_snapshot_latency_ns", 2_500_000)

            _, _, body = await _request(port, "/metrics")
            assert (
                'tickforge_snapshot_requests_total{status="ok"} 1.0' in body
            )
            assert "# TYPE tickforge_snapshot_latency_ns summary" in body
            assert "tickforge_snapshot_latency_ns_count 1.0" in body

            _, _, status_body = await _request(port, "/status")
            snapshot = json.loads(status_body)
            assert snapshot["counters"][
                'tickforge_snapshot_requests_total{status="ok"}'
            ] == 1.0
            assert (
                snapshot["histograms"]["tickforge_snapshot_latency_ns"]["count"]
                == 1
            )
        finally:
            await server.stop()

    run(scenario())
