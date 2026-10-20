"""Binance spot REST depth-snapshot client.

The order book needs a REST snapshot to synchronise with the diff stream.  The
client keeps the exchange's decimal *strings* intact: the snapshot is written
to the WAL in exactly the form Binance returned, so a replay can re-derive the
same integer ticks with the same parser as the live path.

Retry policy
------------
* ``cfg.snapshot_retries`` retries follow the initial attempt (so at most
  ``snapshot_retries + 1`` requests are made).
* Transport failures, timeouts, HTTP 5xx and HTTP 429 are retried with
  exponential backoff starting at ``cfg.snapshot_retry_backoff_s``.
* Other 4xx responses and structurally invalid payloads fail immediately --
  retrying a malformed request or payload only wastes rate-limit budget.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import aiohttp
import orjson

from ..config import FeedConfig
from ..normalize import ParseError, str_to_ticks

__all__ = ["SnapshotClient", "SnapshotData", "SnapshotError"]

#: Sent with every REST request; exchanges use it for traffic attribution.
USER_AGENT = "TickForge/0.1 (market-data demo)"

_MAX_ERROR_BODY_CHARS = 200

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SnapshotData:
    """A validated REST depth snapshot with raw decimal strings preserved.

    ``bids``/``asks`` are ``[[price, qty], ...]`` exactly as returned by
    Binance (strings, not floats).  ``requested_mono_ns`` is sampled before the
    request is issued; the other two timestamps are sampled as soon as the
    response body has been read.  All timestamps are for latency accounting
    only and never take part in derived output.
    """

    symbol: str
    last_update_id: int
    bids: list[list[str]]
    asks: list[list[str]]
    requested_mono_ns: int
    recv_mono_ns: int
    recv_wall_ns: int


class SnapshotError(Exception):
    """Raised when a depth snapshot cannot be fetched or validated."""


def _validate_levels(raw: Any, side: str) -> list[list[str]]:
    """Validate and return one side of the book, preserving raw strings."""

    if not isinstance(raw, list) or not raw:
        raise SnapshotError(f"snapshot {side} must be a non-empty list")
    levels: list[list[str]] = []
    for index, level in enumerate(raw):
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            raise SnapshotError(f"invalid {side} level at index {index}: {level!r}")
        price, qty = level[0], level[1]
        if not isinstance(price, str) or not isinstance(qty, str):
            # Binance always quotes with strings; silently coercing numbers
            # would break the "raw payload" WAL contract.
            raise SnapshotError(
                f"invalid {side} level at index {index}: expected strings, "
                f"got {type(price).__name__}/{type(qty).__name__}"
            )
        try:
            str_to_ticks(price)
            str_to_ticks(qty)
        except ParseError as exc:
            raise SnapshotError(
                f"invalid {side} level at index {index}: {exc}"
            ) from exc
        levels.append([price, qty])
    return levels


def _parse_snapshot(
    symbol: str,
    body: bytes,
    *,
    requested_mono_ns: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> SnapshotData:
    """Parse and validate a ``/api/v3/depth`` response body."""

    try:
        doc = orjson.loads(body)
    except orjson.JSONDecodeError as exc:
        raise SnapshotError(f"snapshot response is not valid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise SnapshotError("snapshot response is not a JSON object")

    raw_update_id = doc.get("lastUpdateId")
    if isinstance(raw_update_id, bool) or not isinstance(raw_update_id, (int, str)):
        raise SnapshotError(f"invalid lastUpdateId: {raw_update_id!r}")
    try:
        last_update_id = int(raw_update_id)
    except (TypeError, ValueError) as exc:
        raise SnapshotError(f"invalid lastUpdateId: {raw_update_id!r}") from exc
    if last_update_id < 0:
        raise SnapshotError(f"invalid lastUpdateId: {last_update_id}")

    bids = _validate_levels(doc.get("bids"), "bids")
    asks = _validate_levels(doc.get("asks"), "asks")

    return SnapshotData(
        symbol=symbol,
        last_update_id=last_update_id,
        bids=bids,
        asks=asks,
        requested_mono_ns=requested_mono_ns,
        recv_mono_ns=recv_mono_ns,
        recv_wall_ns=recv_wall_ns,
    )


class SnapshotClient:
    """Fetch depth snapshots from Binance's REST API.

    The underlying :class:`aiohttp.ClientSession` is created lazily on the
    first fetch and reused afterwards.  The client is not thread-safe; use it
    from the asyncio event loop only.

    ``metrics`` is duck-typed and may be ``None``.  It must expose
    ``inc(name, labels, value)`` and ``observe_ns(name, value, labels)``.
    """

    def __init__(
        self,
        cfg: FeedConfig,
        *,
        logger: logging.Logger | None = None,
        metrics: Any | None = None,
    ) -> None:
        self._cfg = cfg
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._metrics = metrics
        self._session: aiohttp.ClientSession | None = None
        self._closed = False

    async def fetch(self, symbol: str) -> SnapshotData:
        """Fetch, validate and return a depth snapshot for ``symbol``.

        Raises :class:`SnapshotError` after the retry budget is exhausted, on
        a non-retryable HTTP status, or when the payload fails validation.
        """

        if not isinstance(symbol, str) or not symbol.strip():
            raise SnapshotError("symbol must be a non-empty string")
        sym = symbol.strip().upper()
        if self._closed:
            raise SnapshotError("SnapshotClient is closed")

        session = self._ensure_session()
        url = (
            f"{self._cfg.rest_url.rstrip('/')}/api/v3/depth"
            f"?symbol={quote(sym, safe='')}&limit={int(self._cfg.snapshot_limit)}"
        )
        retries = max(0, int(self._cfg.snapshot_retries))
        max_attempts = retries + 1
        backoff = max(0.0, float(self._cfg.snapshot_retry_backoff_s))
        last_error: SnapshotError | None = None

        for attempt in range(max_attempts):
            requested_mono_ns = time.perf_counter_ns()
            try:
                async with session.get(url) as response:
                    body = await response.read()
                    # Sample both clocks as soon as the body is complete and
                    # before any JSON work.
                    recv_mono_ns = time.perf_counter_ns()
                    recv_wall_ns = time.time_ns()
                    status = response.status
            except TimeoutError as exc:
                self._inc_request(sym, "timeout")
                last_error = SnapshotError(
                    f"depth request for {sym} timed out after "
                    f"{self._cfg.snapshot_timeout_s:.3f} s"
                )
                last_error.__cause__ = exc
            except aiohttp.ClientError as exc:
                self._inc_request(sym, "network_error")
                last_error = SnapshotError(
                    f"depth request for {sym} failed: {type(exc).__name__}: {exc}"
                )
                last_error.__cause__ = exc
            except RuntimeError as exc:
                if self._closed:
                    raise SnapshotError(
                        f"SnapshotClient was closed while fetching {sym}"
                    ) from exc
                raise
            else:
                if status == 200:
                    try:
                        data = _parse_snapshot(
                            sym,
                            body,
                            requested_mono_ns=requested_mono_ns,
                            recv_mono_ns=recv_mono_ns,
                            recv_wall_ns=recv_wall_ns,
                        )
                    except SnapshotError:
                        self._inc_request(sym, "invalid")
                        raise
                    self._inc_request(sym, "success")
                    self._observe_latency(recv_mono_ns - requested_mono_ns)
                    self._inc_bytes(len(body))
                    return data

                if 400 <= status < 500 and status != 429:
                    # Client errors won't heal by retrying (bad symbol/limit).
                    self._inc_request(sym, f"http_{status}")
                    detail = _body_preview(body)
                    raise SnapshotError(
                        f"depth request for {sym} rejected with HTTP {status}: "
                        f"{detail}"
                    )

                self._inc_request(sym, f"http_{status}")
                last_error = SnapshotError(
                    f"depth request for {sym} returned HTTP {status}: "
                    f"{_body_preview(body)}"
                )

            if attempt + 1 < max_attempts:
                delay = backoff * (2**attempt)
                if delay > 0.0:
                    await asyncio.sleep(delay)

        if last_error is None:  # defensive: max_attempts >= 1 always fails here
            raise SnapshotError(f"depth request for {sym} failed without a response")
        raise last_error

    async def close(self) -> None:
        """Close the HTTP session.  Idempotent."""

        if self._closed and self._session is None:
            return
        self._closed = True
        session = self._session
        self._session = None
        if session is not None and not session.closed:
            await session.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._closed:
            raise SnapshotError("SnapshotClient is closed")
        session = self._session
        if session is None or session.closed:
            if session is not None and session.closed:
                self._session = None
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=float(self._cfg.snapshot_timeout_s)),
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
            )
            self._session = session
        return session

    def _inc_request(self, symbol: str, status: str) -> None:
        self._inc(
            "tickforge_snapshot_requests_total",
            {"symbol": symbol, "status": status},
        )

    def _observe_latency(self, latency_ns: int) -> None:
        metrics = self._metrics
        if metrics is None:
            return
        try:
            metrics.observe_ns("tickforge_snapshot_latency_ns", latency_ns, None)
        except Exception:  # pragma: no cover - observability is best effort
            self._log.debug("metrics.observe_ns failed", exc_info=True)

    def _inc_bytes(self, value: int) -> None:
        self._inc("tickforge_snapshot_bytes_total", None, float(value))

    def _inc(
        self, name: str, labels: dict[str, str] | None = None, value: float = 1.0
    ) -> None:
        metrics = self._metrics
        if metrics is None:
            return
        try:
            metrics.inc(name, labels, value)
        except Exception:  # pragma: no cover
            self._log.debug("metrics.inc(%s) failed", name, exc_info=True)


def _body_preview(body: bytes) -> str:
    """Return a short, printable preview of an error body for logs/errors."""

    try:
        text = body.decode("utf-8", "replace")
    except Exception:  # pragma: no cover - decode with errors never raises
        return "<unreadable body>"
    text = text.strip()
    if len(text) > _MAX_ERROR_BODY_CHARS:
        text = text[:_MAX_ERROR_BODY_CHARS] + "..."
    return text or "<empty body>"
