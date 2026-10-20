"""Canonical event types shared by the feed, book, factor, strategy and storage layers.

Design notes
------------
* Prices and quantities are stored as integer *ticks* (1e-8 units).  Binance
  spot symbols quote at most 8 decimal places, so this is lossless and avoids
  the usual float-equality problems in an order book.
* ``recv_mono_ns`` is taken from ``time.perf_counter_ns()`` as soon as the
  WebSocket frame is handed to us.  It is used for latency accounting only.
  ``recv_wall_ns`` is ``time.time_ns()`` and is persisted so a replay can
  reproduce time-sensitive bookkeeping without consulting the wall clock.
* Derived records (factor snapshots, signals, fills) deliberately contain no
  receive timestamps.  That keeps their JSON lines byte-for-byte reproducible
  in replay mode, which is what the determinism check hashes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

TICKS_PER_UNIT = 100_000_000  # 1e-8


def ticks_to_float(value: int) -> float:
    """Convert fixed point ticks to a display float (never used as a dict key)."""
    return value / TICKS_PER_UNIT


@dataclass(slots=True, frozen=True)
class DepthEvent:
    """Binance spot ``<symbol>@depth@100ms`` diff update."""

    symbol: str
    event_time_ms: int
    first_update_id: int  # U
    final_update_id: int  # u
    bids: tuple[tuple[int, int], ...]  # (price_ticks, qty_ticks); qty 0 == delete
    asks: tuple[tuple[int, int], ...]
    recv_mono_ns: int
    recv_wall_ns: int
    seq: int  # WAL / ingress sequence number

    @property
    def exchange_seq(self) -> tuple[int, int]:
        return self.first_update_id, self.final_update_id


@dataclass(slots=True, frozen=True)
class TradeEvent:
    """Binance spot ``<symbol>@aggTrade`` event."""

    symbol: str
    event_time_ms: int
    agg_trade_id: int
    price_ticks: int
    qty_ticks: int
    is_buyer_maker: bool
    recv_mono_ns: int
    recv_wall_ns: int
    seq: int

    @property
    def aggressor_side(self) -> str:
        # If the buyer is the maker, the aggressor was a seller.
        return "sell" if self.is_buyer_maker else "buy"

    @property
    def price(self) -> float:
        return ticks_to_float(self.price_ticks)

    @property
    def qty(self) -> float:
        return ticks_to_float(self.qty_ticks)


@dataclass(slots=True, frozen=True)
class BookTickerEvent:
    """Binance spot ``<symbol>@bookTicker`` best bid/ask stream.

    This stream does not carry an event time in the payload, so ``event_time_ms``
    is derived from the receive wall clock and is *not* used for derived output.
    """

    symbol: str
    update_id: int
    bid_px_ticks: int
    bid_qty_ticks: int
    ask_px_ticks: int
    ask_qty_ticks: int
    recv_mono_ns: int
    recv_wall_ns: int
    seq: int

    @property
    def event_time_ms(self) -> int:
        return self.recv_wall_ns // 1_000_000


@dataclass(slots=True, frozen=True)
class SnapshotEvent:
    """Order-book snapshot obtained from the REST API and persisted in the WAL.

    Snapshots are first-class events: replaying only the WebSocket diff stream
    is not sufficient to reconstruct a book, so the snapshot is written to the
    WAL in the exact position where it becomes available.  Live and replay
    processing therefore execute the same state machine.
    """

    symbol: str
    last_update_id: int
    bids: tuple[tuple[int, int], ...]
    asks: tuple[tuple[int, int], ...]
    requested_mono_ns: int
    recv_mono_ns: int
    recv_wall_ns: int
    seq: int


@dataclass(slots=True, frozen=True)
class FactorSnapshot:
    """Point-in-time factor values for one symbol.

    The field set is intentionally small and interpretable.  Factors are used
    only to drive the demo strategy; strategy PnL is not the goal of the task.
    """

    symbol: str
    ts_ms: int
    seq: int
    mid: float
    microprice: float
    spread_bps: float
    ofi_1s: float
    depth_imbalance_5: float
    trade_imbalance_1s: float
    realized_vol_bps: float
    momentum_5s: float
    best_bid: float
    best_ask: float
    bid_qty: float
    ask_qty: float

    def to_dict(self) -> dict[str, Any]:
        # Keep order stable: the JSONL digest depends on it.
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "seq": self.seq,
            "mid": self.mid,
            "microprice": self.microprice,
            "spread_bps": self.spread_bps,
            "ofi_1s": self.ofi_1s,
            "depth_imbalance_5": self.depth_imbalance_5,
            "trade_imbalance_1s": self.trade_imbalance_1s,
            "realized_vol_bps": self.realized_vol_bps,
            "momentum_5s": self.momentum_5s,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "bid_qty": self.bid_qty,
            "ask_qty": self.ask_qty,
        }


@dataclass(slots=True, frozen=True)
class Signal:
    """A two-sided quote proposal emitted by the demo strategy."""

    symbol: str
    ts_ms: int
    seq: int
    fair_price: float
    bid_price: float | None
    bid_qty: float
    ask_price: float | None
    ask_qty: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "seq": self.seq,
            "fair_price": self.fair_price,
            "bid_price": self.bid_price,
            "bid_qty": self.bid_qty,
            "ask_price": self.ask_price,
            "ask_qty": self.ask_qty,
            "reason": self.reason,
        }


@dataclass(slots=True, frozen=True)
class Fill:
    """A simulated maker fill emitted by the execution simulator."""

    symbol: str
    ts_ms: int
    trade_id: int
    side: str  # BUY / SELL
    price: float
    qty: float
    fee: float
    position: float
    cash: float
    realized_pnl: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts_ms": self.ts_ms,
            "trade_id": self.trade_id,
            "side": self.side,
            "price": self.price,
            "qty": self.qty,
            "fee": self.fee,
            "position": self.position,
            "cash": self.cash,
            "realized_pnl": self.realized_pnl,
            "reason": self.reason,
        }


__all__ = [
    "TICKS_PER_UNIT",
    "ticks_to_float",
    "DepthEvent",
    "TradeEvent",
    "BookTickerEvent",
    "SnapshotEvent",
    "FactorSnapshot",
    "Signal",
    "Fill",
]
