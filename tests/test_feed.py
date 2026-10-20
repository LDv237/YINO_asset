"""Tests for :mod:`tickforge.feed.binance`.

Every test runs a real ``websockets`` server bound to ``127.0.0.1:0``.  No
public network access is performed.  The suite exercises frame delivery,
timestamp placement, callback isolation, reconnect behaviour, staleness
detection and stop semantics.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from websockets.asyncio.server import serve

from tickforge.config import FeedConfig
from tickforge.feed.binance import BinanceFeed, FeedError, FeedStale, _backoff_delay

# A non-ASCII payload proves text frames are converted to UTF-8 bytes exactly.
TEXT_PAYLOAD = (
    '{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT",'
    '"p":"1.0","q":"2.0","m":false,"a":1,"E":1,"note":"中文-ß-🎯"}}'
)
BINARY_PAYLOAD = b"\x00\x01\xffraw-binary-frame\x80"

Handler = Callable[[Any], Awaitable[None]]


def make_cfg(port: int, **overrides: Any) -> FeedConfig:
    """Build a loopback-only feed configuration with fast retries."""

    values: dict[str, Any] = dict(
        ws_url=f"ws://127.0.0.1:{port}/stream",
        symbols=["BTCUSDT"],
        depth_speed_ms=100,
        stale_timeout_ms=2_000,
        open_timeout_s=3.0,
        close_timeout_s=1.0,
        ping_interval_s=20.0,
        ping_timeout_s=20.0,
        reconnect_initial_s=0.01,
        reconnect_max_s=0.05,
        message_max_bytes=1024 * 1024,
        snapshot_retries=1,
        snapshot_retry_backoff_s=0.0,
    )
    values.update(overrides)
    return FeedConfig(**values)


class FakeMetrics:
    """Records duck-typed metric calls for assertions."""

    def __init__(self) -> None:
        self.increments: list[tuple[str, dict[str, str], float]] = []
        self.observations: list[tuple[str, int, dict[str, str]]] = []
        self.gauges: list[tuple[str, float, dict[str, str]]] = []

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
    ) -> None:
        self.gauges.append((name, float(value), dict(labels or {})))

    def counter(self, name: str) -> float:
        return sum(value for n, _, value in self.increments if n == name)


class BrokenMetrics:
    """A metrics backend whose every call fails; the feed must survive it."""

    def inc(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")

    def observe_ns(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")

    def set_gauge(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("metrics backend is down")


@asynccontextmanager
async def running_server(handler: Handler) -> AsyncIterator[tuple[Any, int]]:
    server = await serve(handler, "127.0.0.1", 0, ping_interval=None)
    try:
        port = server.sockets[0].getsockname()[1]
        yield server, port
    finally:
        server.close()
        await server.wait_closed()


async def wait_until(
    predicate: Callable[[], bool], *, timeout: float = 5.0, interval: float = 0.005
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was not satisfied before the timeout")


async def stop_feed(feed: BinanceFeed, task: "asyncio.Task[None]") -> None:
    """Stop a feed and wait for its run task, propagating unexpected errors."""

    await feed.stop()
    await asyncio.wait_for(task, timeout=3.0)


@pytest.mark.asyncio
async def test_message_delivery_timestamps_and_raw_bytes() -> None:
    paths: list[str] = []
    user_agents: list[str] = []
    received: asyncio.Queue[tuple[bytes, int, int, int, int]] = asyncio.Queue()

    async def handler(ws: Any) -> None:
        paths.append(ws.request.path)
        user_agents.append(ws.request.headers.get("User-Agent", ""))
        await ws.send(TEXT_PAYLOAD)
        await ws.send(BINARY_PAYLOAD)
        await ws.wait_closed()

    async def on_message(payload: bytes, wall_ns: int, mono_ns: int) -> None:
        entry_wall = time.time_ns()
        entry_mono = time.perf_counter_ns()
        await received.put((payload, wall_ns, mono_ns, entry_wall, entry_mono))

    metrics = FakeMetrics()
    async with running_server(handler) as (_, port):
        feed = BinanceFeed(make_cfg(port), on_message=on_message, metrics=metrics)
        task = asyncio.create_task(feed.run())
        try:
            first = await asyncio.wait_for(received.get(), timeout=5.0)
            second = await asyncio.wait_for(received.get(), timeout=5.0)

            payload, wall_ns, mono_ns, entry_wall, entry_mono = first
            assert payload == TEXT_PAYLOAD.encode("utf-8")
            assert isinstance(wall_ns, int) and isinstance(mono_ns, int)
            # Timestamps are taken before the callback is awaited.
            assert wall_ns <= entry_wall
            assert mono_ns <= entry_mono
            # ... and they are plausible wall/monotonic clock readings.
            assert abs(entry_wall - wall_ns) < 2_000_000_000
            assert wall_ns > 1_600_000_000_000_000_000
            assert 0 < mono_ns < time.perf_counter_ns()

            assert second[0] == BINARY_PAYLOAD
            assert second[2] > mono_ns  # monotonic receive timestamps

            assert feed.connected is True
            assert feed.connections == 1
            assert feed.messages >= 2
            assert feed.last_msg_mono_ns == second[2]

            # The configured combined stream URL is actually used.
            assert paths and "streams=" in paths[0]
            assert "btcusdt@depth@100ms" in paths[0]
            assert user_agents and user_agents[0].startswith("TickForge/")

            assert metrics.counter("tickforge_ws_messages_total") >= 2
            assert metrics.counter("tickforge_ws_bytes_total") == (
                len(TEXT_PAYLOAD.encode("utf-8")) + len(BINARY_PAYLOAD)
            )
            assert metrics.counter("tickforge_ws_connect_total") == 1
        finally:
            await stop_feed(feed, task)

    assert feed.connected is False
    assert metrics.counter("tickforge_ws_disconnect_total") == 1


@pytest.mark.asyncio
async def test_reconnects_after_server_close() -> None:
    connect_events: list[tuple[int, bool]] = []
    disconnect_events: list[tuple[int, str]] = []
    server_connections = 0

    async def handler(ws: Any) -> None:
        nonlocal server_connections
        server_connections += 1
        current = server_connections
        await ws.send(f'{{"n":{current}}}')
        if current == 1:
            await ws.close(code=1011, reason="mock failure")
        else:
            await ws.wait_closed()

    async def on_connect(conn_id: int, is_reconnect: bool) -> None:
        connect_events.append((conn_id, is_reconnect))

    async def on_disconnect(conn_id: int, reason: str) -> None:
        disconnect_events.append((conn_id, reason))

    metrics = FakeMetrics()
    async with running_server(handler) as (_, port):
        feed = BinanceFeed(
            make_cfg(port),
            on_message=lambda *_: asyncio.sleep(0),
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            metrics=metrics,
        )
        task = asyncio.create_task(feed.run())
        try:
            await wait_until(lambda: len(connect_events) >= 2)
            assert connect_events[0] == (1, False)
            assert connect_events[1] == (2, True)
            assert disconnect_events
            assert disconnect_events[0][0] == 1
            assert "closed" in disconnect_events[0][1]
            assert feed.connections == 2
            assert feed.stats()["reconnects"] == 1
            assert metrics.counter("tickforge_ws_reconnect_total") == 1
            assert metrics.counter("tickforge_ws_disconnect_total") >= 1
        finally:
            await stop_feed(feed, task)


@pytest.mark.asyncio
async def test_stale_connection_is_replaced() -> None:
    connect_events: list[tuple[int, bool]] = []
    disconnect_events: list[tuple[int, str]] = []

    async def handler(ws: Any) -> None:
        # Never send: the client must give up after stale_timeout_ms.
        await ws.wait_closed()

    async def on_connect(conn_id: int, is_reconnect: bool) -> None:
        connect_events.append((conn_id, is_reconnect))

    async def on_disconnect(conn_id: int, reason: str) -> None:
        disconnect_events.append((conn_id, reason))

    metrics = FakeMetrics()
    async with running_server(handler) as (_, port):
        cfg = make_cfg(port, stale_timeout_ms=150)
        feed = BinanceFeed(
            cfg,
            on_message=lambda *_: asyncio.sleep(0),
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            metrics=metrics,
        )
        task = asyncio.create_task(feed.run())
        try:
            await wait_until(
                lambda: any("stale" in reason for _, reason in disconnect_events)
            )
            await wait_until(lambda: len(connect_events) >= 2)
            assert connect_events[1] == (2, True)
            assert feed.stats()["stale_timeouts"] >= 1
            assert metrics.counter("tickforge_ws_stale_reconnect_total") >= 1
        finally:
            await stop_feed(feed, task)


@pytest.mark.asyncio
async def test_callback_errors_do_not_kill_read_loop() -> None:
    seen: list[bytes] = []

    async def handler(ws: Any) -> None:
        await ws.send("first")
        await asyncio.sleep(0.05)
        await ws.send("second")
        await ws.wait_closed()

    async def on_message(payload: bytes, wall_ns: int, mono_ns: int) -> None:
        seen.append(payload)
        if len(seen) == 1:
            raise RuntimeError("consumer bug on first frame")

    async def on_connect(conn_id: int, is_reconnect: bool) -> None:
        raise RuntimeError("subscriber bug")

    async def on_disconnect(conn_id: int, reason: str) -> None:
        raise RuntimeError("subscriber bug on disconnect")

    metrics = FakeMetrics()
    async with running_server(handler) as (_, port):
        feed = BinanceFeed(
            make_cfg(port),
            on_message=on_message,
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            metrics=metrics,
        )
        task = asyncio.create_task(feed.run())
        try:
            await wait_until(lambda: len(seen) >= 2)
            assert seen[0] == b"first" and seen[1] == b"second"
            assert feed.connections == 1
            assert feed.messages == 2
            assert feed.connected is True
            assert feed.stats()["callback_errors"] >= 2
            assert metrics.counter("tickforge_ws_callback_errors_total") >= 2
        finally:
            await stop_feed(feed, task)

    # The disconnect callback error is counted too and never escapes run().
    assert feed.stats()["callback_errors"] >= 3


@pytest.mark.asyncio
async def test_stop_wakes_backoff_and_is_idempotent() -> None:
    disconnected = asyncio.Event()

    async def handler(ws: Any) -> None:
        await ws.close(code=1000)

    async def on_disconnect(conn_id: int, reason: str) -> None:
        disconnected.set()

    async with running_server(handler) as (_, port):
        # A long backoff proves stop() interrupts the sleep instead of waiting.
        cfg = make_cfg(port, reconnect_initial_s=10.0, reconnect_max_s=10.0)
        feed = BinanceFeed(
            cfg,
            on_message=lambda *_: asyncio.sleep(0),
            on_disconnect=on_disconnect,
        )
        task = asyncio.create_task(feed.run())
        try:
            await asyncio.wait_for(disconnected.wait(), timeout=5.0)
            started = time.monotonic()
            await feed.stop()
            await asyncio.wait_for(task, timeout=2.0)
            elapsed = time.monotonic() - started
            assert elapsed < 1.5, f"stop took {elapsed:.2f}s during backoff"
            assert feed.connected is False
        finally:
            if not task.done():
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

        # Idempotent: a second stop is a no-op and must not raise.
        await feed.stop()
        await feed.stop()


@pytest.mark.asyncio
async def test_stop_cancels_in_flight_handshake() -> None:
    """stop() must not wait for open_timeout on a black-holed handshake."""

    writers: list[asyncio.StreamWriter] = []

    async def black_hole(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        try:
            await reader.read()  # returns when the client aborts the socket
        except Exception:
            pass

    tcp_server = await asyncio.start_server(black_hole, "127.0.0.1", 0)
    port = tcp_server.sockets[0].getsockname()[1]
    cfg = make_cfg(port, open_timeout_s=20.0, reconnect_initial_s=5.0)
    feed = BinanceFeed(cfg, on_message=lambda *_: asyncio.sleep(0))
    task = asyncio.create_task(feed.run())
    try:
        await wait_until(lambda: bool(writers), timeout=3.0)
        started = time.monotonic()
        await feed.stop()
        await asyncio.wait_for(task, timeout=2.0)
        elapsed = time.monotonic() - started
        assert elapsed < 1.5, f"stop took {elapsed:.2f}s during handshake"
        assert feed.connected is False
        assert feed.connections == 0
    finally:
        tcp_server.close()
        for writer in writers:
            writer.close()
        await tcp_server.wait_closed()


@pytest.mark.asyncio
async def test_connect_failures_are_retried_and_run_survives() -> None:
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
    cfg = make_cfg(port, reconnect_initial_s=0.01, reconnect_max_s=0.02)
    feed = BinanceFeed(cfg, on_message=lambda *_: asyncio.sleep(0))
    task = asyncio.create_task(feed.run())
    try:
        await wait_until(lambda: accepts >= 2, timeout=5.0)
        assert task.done() is False
        assert feed.connected is False
        assert feed.connections == 0
    finally:
        await stop_feed(feed, task)
        tcp_server.close()
        await tcp_server.wait_closed()


@pytest.mark.asyncio
async def test_stop_before_run_returns_immediately() -> None:
    feed = BinanceFeed(make_cfg(1), on_message=lambda *_: asyncio.sleep(0))
    await feed.stop()
    await asyncio.wait_for(feed.run(), timeout=1.0)
    assert feed.connected is False
    assert feed.connections == 0


@pytest.mark.asyncio
async def test_run_twice_is_rejected() -> None:
    async def handler(ws: Any) -> None:
        await ws.send("hello")
        await ws.wait_closed()

    async with running_server(handler) as (_, port):
        feed = BinanceFeed(make_cfg(port), on_message=lambda *_: asyncio.sleep(0))
        task = asyncio.create_task(feed.run())
        try:
            await wait_until(lambda: feed.connected)
            with pytest.raises(FeedError):
                await feed.run()
        finally:
            await stop_feed(feed, task)


@pytest.mark.asyncio
async def test_metrics_failures_are_swallowed() -> None:
    async def handler(ws: Any) -> None:
        await ws.send("payload")
        await ws.wait_closed()

    received = asyncio.Event()

    async def on_message(payload: bytes, wall_ns: int, mono_ns: int) -> None:
        received.set()

    async with running_server(handler) as (_, port):
        feed = BinanceFeed(
            make_cfg(port), on_message=on_message, metrics=BrokenMetrics()
        )
        task = asyncio.create_task(feed.run())
        try:
            await asyncio.wait_for(received.wait(), timeout=5.0)
            assert feed.messages == 1
        finally:
            await stop_feed(feed, task)


def test_backoff_delay_growth_cap_and_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "tickforge.feed.binance.random.uniform", lambda low, high: 1.0
    )
    assert _backoff_delay(0, 0.5, 30.0) == pytest.approx(0.5)
    assert _backoff_delay(1, 0.5, 30.0) == pytest.approx(1.0)
    assert _backoff_delay(2, 0.5, 30.0) == pytest.approx(2.0)
    assert _backoff_delay(10, 0.5, 30.0) == pytest.approx(30.0)
    assert _backoff_delay(10_000, 0.5, 30.0) == pytest.approx(30.0)
    assert _backoff_delay(0, 0.0, 30.0) == 0.0

    monkeypatch.setattr(
        "tickforge.feed.binance.random.uniform", lambda low, high: 0.8
    )
    assert _backoff_delay(0, 1.0, 10.0) == pytest.approx(0.8)

    monkeypatch.setattr(
        "tickforge.feed.binance.random.uniform", lambda low, high: 1.2
    )
    assert _backoff_delay(0, 1.0, 10.0) == pytest.approx(1.2)
    # reconnect_max_s is a hard ceiling even with upward jitter.
    assert _backoff_delay(60, 1.0, 10.0) == pytest.approx(10.0)


def test_backoff_delay_property_bounds() -> None:
    """For any attempt, the delay stays inside the documented jitter band."""

    initial, cap = 0.25, 5.0
    for attempt in range(0, 40):
        raw = initial * (2.0**min(attempt, 60))
        base = min(raw, cap)
        for _ in range(20):
            delay = _backoff_delay(attempt, initial, cap)
            assert 0.8 * base - 1e-12 <= delay <= base * 1.2 + 1e-12
            assert delay <= cap + 1e-12


def test_feed_exception_hierarchy() -> None:
    assert issubclass(FeedStale, FeedError)
    assert issubclass(FeedError, Exception)
