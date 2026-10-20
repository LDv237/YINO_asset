"""Small aiohttp observability server.

Endpoints
---------
``GET /healthz``, ``GET /readyz``
    Evaluate ``health_fn()``.  A mapping containing a truthy ``ok`` yields 200;
    anything else (including callback failures) yields 503.  The callback's
    payload is returned as JSON.
``GET /status``
    Return ``status_fn()`` as JSON (500 if the callback raises or the payload
    is not JSON serializable).
``GET /metrics``
    Prometheus text exposition (version 0.0.4).
``GET /``
    Plain-text list of endpoints.

``start()`` and ``stop()`` are idempotent.  Passing ``port=0`` binds an
ephemeral port; the actual port is available from :attr:`HttpServer.port`.
A bind failure (for example a port already in use) propagates to the caller so
the process can fail fast.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Mapping
from contextlib import suppress
from typing import Any

from aiohttp import web

from ..metrics import MetricsRegistry, NullMetrics

__all__ = ["HttpServer", "METRICS_CONTENT_TYPE"]

#: Exact content type required for Prometheus 0.0.4 text exposition.
METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_ROOT_TEXT = (
    "TickForge observability server\n"
    "\n"
    "Endpoints:\n"
    "  GET /healthz  liveness probe (200/503)\n"
    "  GET /readyz   readiness probe (200/503)\n"
    "  GET /status   JSON runtime status\n"
    "  GET /metrics  Prometheus text exposition\n"
)

_JSON_CT = "application/json"
_TEXT_CT = "text/plain; charset=utf-8"


class HttpServer:
    """Lifecycle wrapper around an aiohttp application runner.

    Args:
        metrics: Object exposing ``render_prometheus()`` (a
            :class:`~tickforge.metrics.MetricsRegistry` or
            :class:`~tickforge.metrics.NullMetrics`).
        health_fn: Synchronous (or awaitable) callback returning a mapping
            such as ``{"ok": True, "wal_lag_ms": 3}``.
        status_fn: Synchronous (or awaitable) callback returning a JSON
            serializable status document.
        host: Bind address.  Keep the default loopback address unless the
            demo is intentionally exposed.
        port: Bind port; ``0`` selects a free ephemeral port.
        logger: Optional logger; defaults to the module logger.
    """

    def __init__(
        self,
        metrics: MetricsRegistry | NullMetrics,
        *,
        health_fn: Callable[[], dict],
        status_fn: Callable[[], dict],
        host: str = "127.0.0.1",
        port: int = 9100,
        logger: logging.Logger | None = None,
    ) -> None:
        self._metrics = metrics
        self._health_fn = health_fn
        self._status_fn = status_fn
        self._host = host
        self._configured_port = int(port)
        self._port = int(port)
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._lifecycle_lock: asyncio.Lock | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @property
    def port(self) -> int:
        """Bound port while running, or the configured port before ``start``."""

        return self._port

    @property
    def started(self) -> bool:
        """Whether the server is currently running."""

        return self._runner is not None

    def _lock(self) -> asyncio.Lock:
        # Created lazily inside the running event loop; this keeps HttpServer
        # constructible outside asyncio.run() and avoids cross-loop bindings.
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        return self._lifecycle_lock

    async def start(self) -> None:
        """Bind the socket and start serving.  Calling twice is a no-op.

        Raises:
            OSError: If the address/port cannot be bound (for example the port
                is already in use).
        """

        async with self._lock():
            if self._runner is not None:
                return

            app = web.Application()
            app.router.add_get("/", self._handle_root)
            app.router.add_get("/healthz", self._handle_health)
            app.router.add_get("/readyz", self._handle_health)
            app.router.add_get("/status", self._handle_status)
            app.router.add_get("/metrics", self._handle_metrics)

            runner = web.AppRunner(app, access_log=None)
            site: web.TCPSite | None = None
            try:
                await runner.setup()
                site = web.TCPSite(
                    runner, host=self._host, port=self._configured_port
                )
                await site.start()
            except BaseException:
                # Never leak a half-initialized runner when binding fails.
                with suppress(Exception):
                    await runner.cleanup()
                raise

            addresses = runner.addresses
            if addresses:
                self._port = int(addresses[0][1])
            else:  # pragma: no cover - defensive fallback for older aiohttp
                server = getattr(site, "_server", None)
                sockets = getattr(server, "sockets", None) if server else None
                if sockets:
                    self._port = int(sockets[0].getsockname()[1])

            self._runner = runner
            self._site = site
            self._logger.info(
                "observability server listening on http://%s:%d",
                self._host,
                self._port,
            )

    async def stop(self) -> None:
        """Stop serving and release the socket.  Safe to call repeatedly."""

        async with self._lock():
            runner = self._runner
            site = self._site
            self._runner = None
            self._site = None
            if runner is None:
                return
            try:
                if site is not None:
                    await site.stop()
            except Exception:  # pragma: no cover - shutdown best effort
                self._logger.exception("error while stopping HTTP site")
            try:
                await runner.cleanup()
            except Exception:  # pragma: no cover - shutdown best effort
                self._logger.exception("error while cleaning up HTTP runner")
            self._port = self._configured_port
            self._logger.info("observability server stopped")

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------
    async def _handle_root(self, request: web.Request) -> web.Response:
        return web.Response(
            body=_ROOT_TEXT.encode("utf-8"),
            headers={"Content-Type": _TEXT_CT},
        )

    async def _handle_health(self, request: web.Request) -> web.Response:
        try:
            payload = await _maybe_await(self._health_fn())
        except Exception:
            self._logger.exception("health callback failed")
            return web.json_response(
                {"ok": False, "error": "health callback failed"}, status=503
            )

        if isinstance(payload, Mapping):
            ok = bool(payload.get("ok", False))
            body: Any = dict(payload)
            status = 200 if ok else 503
        else:
            status = 503
            body = {"ok": False, "detail": str(payload)}

        try:
            return web.json_response(body, status=status)
        except (TypeError, ValueError):
            self._logger.exception("health payload is not JSON serializable")
            return web.json_response(
                {"ok": False, "error": "health payload is not JSON serializable"},
                status=503,
            )

    async def _handle_status(self, request: web.Request) -> web.Response:
        try:
            payload = await _maybe_await(self._status_fn())
        except Exception:
            self._logger.exception("status callback failed")
            return web.json_response(
                {"error": "status callback failed"}, status=500
            )

        try:
            return web.json_response(payload, content_type=_JSON_CT)
        except (TypeError, ValueError):
            self._logger.exception("status payload is not JSON serializable")
            return web.json_response(
                {"error": "status payload is not JSON serializable"}, status=500
            )

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        try:
            body = self._metrics.render_prometheus()
        except Exception:
            self._logger.exception("metrics rendering failed")
            return web.Response(
                body=b"metrics rendering failed\n",
                status=500,
                headers={"Content-Type": _TEXT_CT},
            )
        return web.Response(
            body=str(body).encode("utf-8"),
            headers={"Content-Type": METRICS_CONTENT_TYPE},
        )


async def _maybe_await(value: Any) -> Any:
    """Await ``value`` when it is awaitable, otherwise return it unchanged."""

    if inspect.isawaitable(value):
        return await value
    return value
