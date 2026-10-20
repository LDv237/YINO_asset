"""Parsing and encoding of exchange payloads into canonical events.

The hot path uses ``orjson`` and integer tick conversion.  Everything here is
pure and synchronous so it can be exercised deterministically from tests and
from the replay path.
"""

from __future__ import annotations

import orjson

from .events import (
    BookTickerEvent,
    DepthEvent,
    SnapshotEvent,
    TradeEvent,
)

_TICK_DECIMALS = 8
_TICK_FACTOR = 10**_TICK_DECIMALS


class ParseError(ValueError):
    """Raised when a frame or snapshot payload is structurally invalid."""


def str_to_ticks(value: str | int) -> int:
    """Convert a Binance decimal string (e.g. ``"81574.38000000"``) to ticks.

    ``int`` input is treated as already being in ticks.  The function is
    branch-light on the common path (8 decimal places, no sign) and is used
    for every price and quantity in the system.
    """

    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ParseError(f"not a decimal string: {value!r}")

    s = value.strip()
    if not s:
        raise ParseError("empty decimal string")
    negative = s[0] == "-"
    if negative:
        s = s[1:]
        if not s or s == ".":
            raise ParseError(f"invalid decimal string: {value!r}")
    whole, dot, frac = s.partition(".")
    if not whole:
        whole = "0"
    if not whole.isdigit() or (dot and not frac.isdigit()):
        raise ParseError(f"invalid decimal string: {value!r}")
    if dot and len(frac) > _TICK_DECIMALS:
        # Binance never sends more than 8 dp; reject rather than silently
        # losing precision (a silent round would corrupt the book).
        raise ParseError(f"more than {_TICK_DECIMALS} decimals: {value!r}")
    frac = frac.ljust(_TICK_DECIMALS, "0") if dot else "0" * _TICK_DECIMALS
    ticks = int(whole) * _TICK_FACTOR + int(frac)
    return -ticks if negative else ticks


def ticks_to_str(value: int) -> str:
    """Inverse of :func:`str_to_ticks`, usable for human-readable logs."""

    sign = "-" if value < 0 else ""
    value = abs(value)
    return f"{sign}{value // _TICK_FACTOR}.{value % _TICK_FACTOR:08d}"


def parse_frame(payload: bytes) -> tuple[str, dict]:
    """Decode a combined-stream frame and return ``(stream, data)``.

    If the payload is a raw (non-combined) stream event, ``stream`` is an empty
    string and the caller can infer it from ``data["e"]`` / ``data["s"]``.
    """

    try:
        doc = orjson.loads(payload)
    except orjson.JSONDecodeError as exc:
        raise ParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(doc, dict):
        raise ParseError("frame is not a JSON object")
    stream = doc.get("stream") or ""
    data = doc.get("data")
    if not isinstance(data, dict):
        data = doc
    if not isinstance(data, dict):
        raise ParseError("frame data is not an object")
    return stream, data


def _symbol(data: dict) -> str:
    symbol = data.get("s")
    if not isinstance(symbol, str) or not symbol:
        raise ParseError("missing symbol")
    return symbol.upper()


def _levels(raw: object) -> tuple[tuple[int, int], ...]:
    if not isinstance(raw, list):
        raise ParseError("book side is not a list")
    out: list[tuple[int, int]] = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            raise ParseError(f"invalid book level: {item!r}")
        price = str_to_ticks(item[0])
        qty = str_to_ticks(item[1])
        out.append((price, qty))
    return tuple(out)


def parse_depth(
    data: dict,
    *,
    seq: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> DepthEvent:
    try:
        first = int(data["U"])
        final = int(data["u"])
        event_time = int(data["E"])
        symbol = _symbol(data)
        bids = _levels(data.get("b", []))
        asks = _levels(data.get("a", []))
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid depth event: {exc}") from exc
    if final < first:
        raise ParseError(f"depth event has u < U: {first}..{final}")
    return DepthEvent(
        symbol=symbol,
        event_time_ms=event_time,
        first_update_id=first,
        final_update_id=final,
        bids=bids,
        asks=asks,
        recv_mono_ns=recv_mono_ns,
        recv_wall_ns=recv_wall_ns,
        seq=seq,
    )


def parse_trade(
    data: dict,
    *,
    seq: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> TradeEvent:
    try:
        symbol = _symbol(data)
        event_time = int(data["E"])
        agg_id = int(data["a"])
        price = str_to_ticks(data["p"])
        qty = str_to_ticks(data["q"])
        maker = bool(data["m"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid aggTrade event: {exc}") from exc
    if price <= 0 or qty <= 0:
        raise ParseError("trade price/qty must be positive")
    return TradeEvent(
        symbol=symbol,
        event_time_ms=event_time,
        agg_trade_id=agg_id,
        price_ticks=price,
        qty_ticks=qty,
        is_buyer_maker=maker,
        recv_mono_ns=recv_mono_ns,
        recv_wall_ns=recv_wall_ns,
        seq=seq,
    )


def parse_book_ticker(
    data: dict,
    *,
    seq: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> BookTickerEvent:
    try:
        symbol = _symbol(data)
        update_id = int(data["u"])
        bid_px = str_to_ticks(data["b"])
        bid_qty = str_to_ticks(data["B"])
        ask_px = str_to_ticks(data["a"])
        ask_qty = str_to_ticks(data["A"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid bookTicker event: {exc}") from exc
    return BookTickerEvent(
        symbol=symbol,
        update_id=update_id,
        bid_px_ticks=bid_px,
        bid_qty_ticks=bid_qty,
        ask_px_ticks=ask_px,
        ask_qty_ticks=ask_qty,
        recv_mono_ns=recv_mono_ns,
        recv_wall_ns=recv_wall_ns,
        seq=seq,
    )


def parse_ws_message(
    payload: bytes,
    *,
    seq: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> DepthEvent | TradeEvent | BookTickerEvent | None:
    """Parse one WS frame into the matching event type.

    Returns ``None`` for streams we subscribe to but do not model.  Structural
    errors raise :class:`ParseError`; callers must persist the raw frame either
    way and count the parse error.
    """

    stream, data = parse_frame(payload)
    event_type = str(data.get("e", ""))
    sl = stream.lower()
    if event_type == "depthUpdate" or "@depth" in sl:
        return parse_depth(
            data, seq=seq, recv_mono_ns=recv_mono_ns, recv_wall_ns=recv_wall_ns
        )
    if event_type == "aggTrade" or sl.endswith("@aggtrade"):
        return parse_trade(
            data, seq=seq, recv_mono_ns=recv_mono_ns, recv_wall_ns=recv_wall_ns
        )
    if event_type == "bookTicker" or sl.endswith("@bookticker"):
        return parse_book_ticker(
            data, seq=seq, recv_mono_ns=recv_mono_ns, recv_wall_ns=recv_wall_ns
        )
    return None


def encode_snapshot(
    *,
    symbol: str,
    last_update_id: int,
    bids: list[list[str]],
    asks: list[list[str]],
    requested_mono_ns: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> bytes:
    """Serialize a REST snapshot for the WAL (kind=snapshot record)."""

    doc = {
        "symbol": symbol.upper(),
        "lastUpdateId": int(last_update_id),
        "bids": bids,
        "asks": asks,
        "requested_mono_ns": int(requested_mono_ns),
        "recv_mono_ns": int(recv_mono_ns),
        "recv_wall_ns": int(recv_wall_ns),
    }
    return orjson.dumps(doc)


def decode_snapshot(
    payload: bytes,
    *,
    seq: int,
    recv_mono_ns: int,
    recv_wall_ns: int,
) -> SnapshotEvent:
    """Decode a snapshot WAL record into the canonical event."""

    try:
        doc = orjson.loads(payload)
        symbol = str(doc["symbol"]).upper()
        last_update_id = int(doc["lastUpdateId"])
        bids = _levels(doc["bids"])
        asks = _levels(doc["asks"])
        requested = int(doc.get("requested_mono_ns", 0))
    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ParseError(f"invalid snapshot payload: {exc}") from exc
    if not bids or not asks:
        raise ParseError("snapshot must contain both sides")
    return SnapshotEvent(
        symbol=symbol,
        last_update_id=last_update_id,
        bids=bids,
        asks=asks,
        requested_mono_ns=requested,
        recv_mono_ns=recv_mono_ns,
        recv_wall_ns=recv_wall_ns,
        seq=seq,
    )


__all__ = [
    "ParseError",
    "str_to_ticks",
    "ticks_to_str",
    "parse_frame",
    "parse_depth",
    "parse_trade",
    "parse_book_ticker",
    "parse_ws_message",
    "encode_snapshot",
    "decode_snapshot",
]
