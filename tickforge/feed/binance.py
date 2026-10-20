"""Resilient Binance spot combined-stream WebSocket feed.

The reader is deliberately thin: it owns the connection lifecycle (connect,
read, detect staleness, reconnect with jittered exponential backoff) and hands
each frame to ``on_message`` as raw bytes together with two receive
timestamps.  Parsing, persistence and book maintenance are somebody else's
job -- see :mod:`tickforge.normalize` and the ingress pipeline.

Timestamp contract
------------------
``recv_wall_ns`` and ``recv_mono_ns`` are sampled immediately after
``ws.recv()`` returns, *before* awaiting the user callback.  That keeps the
latency accounting honest when a slow consumer delays the event loop.  The
payload itself is preserved byte-for-byte (text frames are UTF-8 encoded, which
is the inverse of how Binance sends JSON).

The feed never consults the wall clock to make routing decisions.  The only
randomness is the reconnect jitter, which cannot influence any derived factor,
signal or fill.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from ..config import FeedConfig

__all__ = ["BinanceFeed", "FeedError", "FeedStale"]

#: Sent to Binance so connections can be attributed in server-side logs.
USER_AGENT = "TickForge/0.1 (market-data demo)"

#: Callback type aliases, kept local to document the public contract.
MessageCallback = Callable[[bytes, int, int], Awaitable[None]]
ConnectCallback = Callable[[int, bool], Awaitable[None]]
DisconnectCallback = Callable[[int, str], Awaitable[None]]

logger = logging.getLogger(__name__)


class FeedError(Exception):
    """Base class for WebSocket feed failures."""


class FeedStale(FeedError):
    """Raised internally when no frame arrives within ``stale_timeout_ms``."""


def _closed_reason(exc: ConnectionClosed) -> str:
    """Format a :class:`ConnectionClosed` without deprecated properties."""

    rcvd = exc.rcvd
    if rcvd is None:  # no close frame (TCP reset / abort)
        return "closed(no-close-frame)"
    return f"closed(code={rcvd.code}, reason={rcvd.reason!r})"


def _backoff_delay(attempt: int, initial_s: float, max_s: float) -> float:
    """Return the reconnect delay for a zero-based ``attempt``.

    The growth is ``initial_s * 2**attempt`` capped at ``max_s`` and then
    multiplied by a uniform jitter in ``[0.8, 1.2]``.  Jitter desynchronises
    many clients reconnecting after an exchange-side restart.  ``max_s`` is a
    hard ceiling: the jittered result is clamped as well, so a deployment can
    rely on the configured maximum.
    """

    if initial_s <= 0.0:
        return 0.0
    # Clamp the exponent to avoid float overflow on pathological attempt
    # counts (the loop should never get there, but this function is public
    # enough to be called from tests and diagnostics).
    exponent = min(max(int(attempt), 0), 60)
    base = initial_s * (2.0**exponent)
    if max_s > 0.0:
        base = min(base, max_s)
    delay = base * random.uniform(0.8, 1.2)
    if max_s > 0.0:
        delay = min(delay, max_s)
    return delay


class BinanceFeed:
    """Read a Binance combined WebSocket stream until :meth:`stop` is called.

    Parameters
    ----------
    cfg:
        Feed configuration.  ``combined_ws_url()`` supplies the endpoint and
        all timers/limits are taken from it.
    on_message:
        Awaited with ``(payload, recv_wall_ns, recv_mono_ns)`` for every frame
        received.  Exceptions are logged and counted; the feed keeps reading.
    on_connect:
        Optional callback invoked as ``(conn_id, is_reconnect)`` after a
        successful handshake.  ``conn_id`` starts at 1 and increases by one
        per successful connection.
    on_disconnect:
        Optional callback invoked as ``(conn_id, reason)`` when an established
        connection ends, before the next reconnect attempt.
    logger:
        Optional :class:`logging.Logger`; defaults to the module logger.
    metrics:
        Optional duck-typed metrics object exposing
        ``inc(name, labels, value)``, ``observe_ns(name, value, labels)`` and
        ``set_gauge(name, value, labels)``.  Metric failures are swallowed so
        observability can never take down the feed.

    Notes
    -----
    ``run()`` owns the task and must be awaited.  ``stop()`` may be called from
    any coroutine; it is idempotent and safe to call before ``run()``.
    """

    def __init__(
        self,
        cfg: FeedConfig,
        *,
        on_message: MessageCallback,
        on_connect: ConnectCallback | None = None,
        on_disconnect: DisconnectCallback | None = None,
        logger: logging.Logger | None = None,
        metrics: Any | None = None,
    ) -> None:
        self._cfg = cfg
        self._on_message = on_message
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._metrics = metrics

        self._stop_event = asyncio.Event()
        self._ws: Any | None = None
        self._connect_task: asyncio.Task[Any] | None = None
        self._running = False

        self._connected = False
        self._connections = 0
        self._messages = 0
        self._bytes = 0
        self._callback_errors = 0
        self._stale_timeouts = 0
        self._reconnects = 0
        self._last_msg_mono_ns = 0

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------
    @property
    def connected(self) -> bool:
        """Whether a WebSocket connection is currently established."""
        return self._connected

    @property
    def last_msg_mono_ns(self) -> int:
        """``perf_counter_ns`` timestamp of the most recently received frame."""
        return self._last_msg_mono_ns

    @property
    def connections(self) -> int:
        """Number of successful connections since construction (1-based ids)."""
        return self._connections

    @property
    def messages(self) -> int:
        """Number of frames received since construction."""
        return self._messages

    def stats(self) -> dict[str, Any]:
        """Return a snapshot of feed counters (useful for logs and tests)."""
        return {
            "connected": self._connected,
            "connections": self._connections,
            "messages": self._messages,
            "bytes": self._bytes,
            "callback_errors": self._callback_errors,
            "stale_timeouts": self._stale_timeouts,
            "reconnects": self._reconnects,
            "last_msg_mono_ns": self._last_msg_mono_ns,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Connect, read frames and reconnect until :meth:`stop` is called.

        The method returns only after the stop flag is set (or, if cancelled,
        after the cancellation has propagated).  Reconnect delays use
        exponential backoff with jitter and are capped at
        ``cfg.reconnect_max_s``.  A connection that receives no frame within
        ``cfg.stale_timeout_ms`` is closed and replaced.
        """

        if self._running:
            raise FeedError("BinanceFeed.run() is already running")
        self._running = True
        attempt = 0
        try:
            while not self._stop_event.is_set():
                # --- connect (cancellable by stop()) --------------------
                connect_task: asyncio.Task[Any] = asyncio.ensure_future(
                    self._open_connection()
                )
                self._connect_task = connect_task
                served = False
                try:
                    ws = await connect_task
                except asyncio.CancelledError:
                    if self._stop_event.is_set():
                        break
                    raise
                except Exception as exc:
                    self._log.warning(
                        "websocket connect failed (attempt %d): %s", attempt + 1, exc
                    )
                else:
                    attempt = 0
                    served = True
                    if self._stop_event.is_set():
                        # stop() won the race with the handshake; do not
                        # briefly advertise a connection nobody can use.
                        try:
                            await ws.close()
                        except Exception:  # pragma: no cover - best effort
                            self._log.debug("error closing unused websocket")
                        break
                    await self._serve_connection(ws)
                    if self._stop_event.is_set():
                        break
                finally:
                    # If run() itself was cancelled while awaiting the
                    # handshake, the inner task is *not* cancelled
                    # automatically.  Cancel it if still pending; if it
                    # completed concurrently, close the late connection so a
                    # cancelled run cannot leak a TCP/TLS socket.
                    pending = self._connect_task
                    if pending is not None:
                        if not pending.done():
                            pending.cancel()
                        elif not served and not pending.cancelled():
                            try:
                                late_ws = pending.result()
                            except BaseException:
                                late_ws = None
                            if late_ws is not None:
                                try:
                                    # Shield the close so the cancellation
                                    # currently propagating cannot abandon it
                                    # halfway through the handshake.
                                    await asyncio.shield(late_ws.close())
                                except asyncio.CancelledError:
                                    raise
                                except Exception:  # pragma: no cover
                                    self._log.debug(
                                        "error closing late websocket", exc_info=True
                                    )
                    self._connect_task = None

                # --- backoff (interruptible by stop()) ------------------
                if self._stop_event.is_set():
                    break
                delay = _backoff_delay(
                    attempt,
                    float(self._cfg.reconnect_initial_s),
                    float(self._cfg.reconnect_max_s),
                )
                attempt += 1
                self._log.info("reconnecting in %.3f s", delay)
                await self._sleep_or_stop(delay)
        finally:
            self._connected = False
            self._ws = None
            self._running = False

    async def stop(self) -> None:
        """Stop the feed.  Idempotent and safe to call from any coroutine.

        Sets the stop flag, cancels an in-flight connect and closes the active
        WebSocket so a blocked ``recv()`` wakes up.  ``run()`` still performs
        the orderly cleanup (``on_disconnect`` etc.) before returning.
        """

        self._stop_event.set()
        self._connected = False

        task = self._connect_task
        if task is not None and not task.done():
            task.cancel()

        ws = self._ws
        if ws is not None:
            try:
                await asyncio.wait_for(
                    ws.close(), timeout=max(0.1, float(self._cfg.close_timeout_s))
                )
            except TimeoutError:
                self._log.warning("timed out closing websocket during stop()")
                # The peer ignored the close handshake; abort the underlying
                # transport so run() cannot linger in another close timeout.
                transport = getattr(ws, "transport", None)
                abort = getattr(transport, "abort", None)
                if callable(abort):
                    try:
                        abort()
                    except Exception:  # pragma: no cover - best effort
                        pass
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - close is best effort
                self._log.debug("error closing websocket during stop()", exc_info=True)

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------
    async def _open_connection(self) -> Any:
        """Create the configured WebSocket connection."""
        cfg = self._cfg
        return await websockets.connect(
            cfg.combined_ws_url(),
            ping_interval=cfg.ping_interval_s,
            ping_timeout=cfg.ping_timeout_s,
            open_timeout=cfg.open_timeout_s,
            close_timeout=cfg.close_timeout_s,
            max_size=cfg.message_max_bytes,
            user_agent_header=USER_AGENT,
        )

    async def _serve_connection(self, ws: Any) -> None:
        """Run one established connection to completion.

        ``ws`` is already connected.  This takes care of the connection
        counters, callbacks, disconnect metrics and closing the socket; it
        never raises for expected connection errors.
        """

        self._ws = ws
        self._connected = True
        self._connections += 1
        conn_id = self._connections
        is_reconnect = conn_id > 1
        if is_reconnect:
            self._reconnects += 1
            self._inc("tickforge_ws_reconnect_total")
        self._inc("tickforge_ws_connect_total")
        self._log.info(
            "websocket connected (conn_id=%d, reconnect=%s)", conn_id, is_reconnect
        )

        reason = "stopped"
        try:
            await self._safe_callback_connect(conn_id, is_reconnect)
            reason = await self._read_loop(ws)
        except FeedStale as exc:
            reason = "stale"
            self._log.warning("connection %d is stale: %s", conn_id, exc)
        except asyncio.CancelledError:
            reason = "cancelled"
            raise
        except ConnectionClosed as exc:  # defensive: normally handled in loop
            reason = _closed_reason(exc)
        except Exception as exc:  # pragma: no cover - unexpected driver errors
            reason = f"error:{type(exc).__name__}:{exc}"
            self._log.exception("unexpected websocket error (conn_id=%d)", conn_id)
        finally:
            self._connected = False
            self._ws = None
            try:
                await ws.close()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - best effort
                self._log.debug("error closing websocket", exc_info=True)
            await self._safe_callback_disconnect(conn_id, reason)
            self._inc("tickforge_ws_disconnect_total")

    async def _read_loop(self, ws: Any) -> str:
        """Read and dispatch frames until the connection ends.

        Returns a human-readable disconnect reason.  Raises :class:`FeedStale`
        when ``stale_timeout_ms`` elapses between frames.
        """

        stale_s = max(0.001, float(self._cfg.stale_timeout_ms) / 1000.0)
        while not self._stop_event.is_set():
            try:
                payload = await asyncio.wait_for(ws.recv(), timeout=stale_s)
            except TimeoutError as exc:
                self._stale_timeouts += 1
                self._inc("tickforge_ws_stale_reconnect_total")
                raise FeedStale(
                    f"no frame within {stale_s:.3f} s (stale_timeout_ms="
                    f"{self._cfg.stale_timeout_ms})"
                ) from exc
            except ConnectionClosed as exc:
                if self._stop_event.is_set():
                    return "stopped"
                return _closed_reason(exc)

            # Sample both clocks before touching the payload or awaiting user
            # code: this is the first instant at which we owned the frame.
            recv_wall_ns = time.time_ns()
            recv_mono_ns = time.perf_counter_ns()

            if isinstance(payload, str):
                raw = payload.encode("utf-8")
            elif isinstance(payload, (bytes, bytearray, memoryview)):
                raw = bytes(payload)
            else:  # pragma: no cover - websockets only yields str/bytes
                self._log.warning("ignoring unsupported websocket frame: %r", type(payload))
                continue

            self._messages += 1
            self._bytes += len(raw)
            self._last_msg_mono_ns = recv_mono_ns
            self._inc("tickforge_ws_messages_total")
            self._inc("tickforge_ws_bytes_total", value=float(len(raw)))

            try:
                await self._on_message(raw, recv_wall_ns, recv_mono_ns)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # A bad consumer must never kill market-data ingestion.
                self._callback_errors += 1
                self._inc("tickforge_ws_callback_errors_total")
                self._log.exception("on_message callback failed: %s", exc)

        return "stopped"

    async def _sleep_or_stop(self, delay: float) -> None:
        """Sleep for ``delay`` seconds, waking early when stop is requested."""
        if delay <= 0.0 or self._stop_event.is_set():
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def _safe_callback_connect(self, conn_id: int, is_reconnect: bool) -> None:
        cb = self._on_connect
        if cb is None:
            return
        try:
            await cb(conn_id, is_reconnect)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._callback_errors += 1
            self._inc("tickforge_ws_callback_errors_total")
            self._log.exception("on_connect callback failed: %s", exc)

    async def _safe_callback_disconnect(self, conn_id: int, reason: str) -> None:
        cb = self._on_disconnect
        if cb is None:
            return
        try:
            await cb(conn_id, reason)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._callback_errors += 1
            self._inc("tickforge_ws_callback_errors_total")
            self._log.exception("on_disconnect callback failed: %s", exc)

    # ------------------------------------------------------------------
    # Metrics plumbing (duck-typed and best effort)
    # ------------------------------------------------------------------
    def _inc(
        self, name: str, labels: dict[str, str] | None = None, value: float = 1.0
    ) -> None:
        metrics = self._metrics
        if metrics is None:
            return
        try:
            metrics.inc(name, labels, value)
        except Exception:  # pragma: no cover - observability is best effort
            self._log.debug("metrics.inc(%s) failed", name, exc_info=True)

