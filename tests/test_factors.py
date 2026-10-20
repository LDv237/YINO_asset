"""Tests for the deterministic microstructure factor engine.

Coverage focuses on the properties that matter for the replay/determinism
contract: hand-computed values, every OFI branch, sliding-window boundaries,
event-time throttle monotonicity, reset semantics, missing/unsynchronised
books, and bounded window memory under a 10k-event stream.  A seeded random
stream doubles as a finite-output property check and a byte-level determinism
check (two identical runs must produce identical snapshots).
"""

from __future__ import annotations

import math
import random

import pytest

from tickforge.book import OrderBook
from tickforge.events import (
    TICKS_PER_UNIT,
    DepthEvent,
    FactorSnapshot,
    SnapshotEvent,
    TradeEvent,
)
from tickforge.factor import FactorEngine

SYMBOL = "BTCUSDT"
T = TICKS_PER_UNIT


# ---------------------------------------------------------------------------
# Event and book helpers
# ---------------------------------------------------------------------------
def snapshot(
    last_update_id: int,
    bids: tuple[tuple[int, int], ...] = (),
    asks: tuple[tuple[int, int], ...] = (),
    *,
    symbol: str = SYMBOL,
    seq: int = 0,
) -> SnapshotEvent:
    """Build a SnapshotEvent with zeroed receive timestamps."""
    return SnapshotEvent(
        symbol=symbol,
        last_update_id=last_update_id,
        bids=tuple(bids),
        asks=tuple(asks),
        requested_mono_ns=0,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=seq,
    )


def depth(
    first_update_id: int,
    final_update_id: int,
    bids: tuple[tuple[int, int], ...] = (),
    asks: tuple[tuple[int, int], ...] = (),
    *,
    event_time_ms: int = 0,
    symbol: str = SYMBOL,
    seq: int = 0,
) -> DepthEvent:
    """Build a DepthEvent with zeroed receive timestamps."""
    return DepthEvent(
        symbol=symbol,
        event_time_ms=event_time_ms,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        bids=tuple(bids),
        asks=tuple(asks),
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=seq,
    )


def make_trade(
    event_time_ms: int,
    qty_ticks: int,
    *,
    is_buyer_maker: bool = False,
    symbol: str = SYMBOL,
    agg_trade_id: int = 0,
    price_ticks: int = 100 * T,
    seq: int = 0,
) -> TradeEvent:
    """Build a TradeEvent with zeroed receive timestamps."""
    return TradeEvent(
        symbol=symbol,
        event_time_ms=event_time_ms,
        agg_trade_id=agg_trade_id,
        price_ticks=price_ticks,
        qty_ticks=qty_ticks,
        is_buyer_maker=is_buyer_maker,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=seq,
    )


class QuoteBook:
    """Synchronised single-level book whose best quote can be moved.

    ``apply`` produces a real diff event, applies it to the underlying
    :class:`OrderBook`, and returns it so the caller can pass the *same* event
    to ``FactorEngine.on_depth`` exactly as the pipeline does.
    """

    def __init__(
        self,
        bid_px: int,
        bid_qty: int,
        ask_px: int,
        ask_qty: int,
        *,
        last_update_id: int = 1,
        symbol: str = SYMBOL,
    ) -> None:
        self.book = OrderBook(symbol)
        result = self.book.apply_snapshot(
            snapshot(
                last_update_id,
                ((bid_px, bid_qty),),
                ((ask_px, ask_qty),),
                symbol=symbol,
            )
        )
        assert result.status == "applied" and result.synced
        self.bid_px = bid_px
        self.bid_qty = bid_qty
        self.ask_px = ask_px
        self.ask_qty = ask_qty
        self.last_update_id = last_update_id

    def apply(
        self,
        event_time_ms: int,
        *,
        bid_px: int | None = None,
        bid_qty: int | None = None,
        ask_px: int | None = None,
        ask_qty: int | None = None,
        seq: int = 0,
    ) -> DepthEvent:
        """Apply a one-tick-uid diff update changing the requested levels."""
        new_bid_px = self.bid_px if bid_px is None else bid_px
        new_bid_qty = self.bid_qty if bid_qty is None else bid_qty
        new_ask_px = self.ask_px if ask_px is None else ask_px
        new_ask_qty = self.ask_qty if ask_qty is None else ask_qty

        bids: list[tuple[int, int]] = []
        if new_bid_px != self.bid_px:
            bids.append((self.bid_px, 0))
        bids.append((new_bid_px, new_bid_qty))

        asks: list[tuple[int, int]] = []
        if new_ask_px != self.ask_px:
            asks.append((self.ask_px, 0))
        asks.append((new_ask_px, new_ask_qty))

        self.last_update_id += 1
        uid = self.last_update_id
        ev = depth(
            uid,
            uid,
            tuple(bids),
            tuple(asks),
            event_time_ms=event_time_ms,
            seq=seq,
        )
        result = self.book.apply_depth(ev)
        assert result.status == "applied" and result.synced, result
        self.bid_px = new_bid_px
        self.bid_qty = new_bid_qty
        self.ask_px = new_ask_px
        self.ask_qty = new_ask_qty
        return ev


def _ofi_after_change(
    prev_quote: tuple[int, int, int, int],
    new_quote: tuple[int, int, int, int],
) -> float:
    """Feed a baseline quote and one changed quote; return OFI_1s in units."""
    engine = FactorEngine(interval_ms=0, ofi_window_ms=1000)
    bid_px, bid_qty, ask_px, ask_qty = prev_quote
    book = QuoteBook(bid_px, bid_qty, ask_px, ask_qty)

    first = engine.on_depth(book.book, book.apply(1000, seq=1))
    assert first is not None
    assert first.ofi_1s == 0.0  # no previous quote -> no synthetic OFI

    bid_px, bid_qty, ask_px, ask_qty = new_quote
    changed = book.apply(
        1001,
        bid_px=bid_px,
        bid_qty=bid_qty,
        ask_px=ask_px,
        ask_qty=ask_qty,
        seq=2,
    )
    snap = engine.on_depth(book.book, changed)
    assert snap is not None
    return snap.ofi_1s


# ---------------------------------------------------------------------------
# Construction and hand-computed values
# ---------------------------------------------------------------------------
def test_constructor_validation() -> None:
    with pytest.raises(ValueError):
        FactorEngine(interval_ms=-1)
    with pytest.raises(ValueError):
        FactorEngine(interval_ms=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        FactorEngine(ofi_window_ms=0)
    with pytest.raises(ValueError):
        FactorEngine(trade_window_ms=0)
    with pytest.raises(ValueError):
        FactorEngine(momentum_window_ms=0)
    with pytest.raises(ValueError):
        FactorEngine(vol_alpha=1.0)
    with pytest.raises(ValueError):
        FactorEngine(vol_alpha=-0.1)
    with pytest.raises(ValueError):
        FactorEngine(vol_alpha=float("nan"))

    # Valid boundary values.
    engine = FactorEngine(
        interval_ms=0,
        ofi_window_ms=1,
        trade_window_ms=1,
        momentum_window_ms=1,
        vol_alpha=0.0,
    )
    assert engine.stats() == {}


def test_hand_computed_microprice_spread_and_depth_imbalance() -> None:
    """Microprice, spread and top-5 imbalance against manual arithmetic."""
    bids = (
        (100 * T, 3 * T),   # best bid: price 100, qty 3
        (99 * T, 4 * T),
        (98 * T, 5 * T),
        (97 * T, 6 * T),
        (96 * T, 7 * T),
        (95 * T, 500 * T),  # outside top 5, must not contribute
    )
    asks = (
        (101 * T, 2 * T),   # best ask: price 101, qty 2
        (102 * T, 3 * T),
        (103 * T, 4 * T),
        (104 * T, 5 * T),
        (105 * T, 6 * T),
        (106 * T, 500 * T),  # outside top 5
    )
    book = OrderBook(SYMBOL)
    assert book.apply_snapshot(snapshot(100, bids, asks)).synced

    engine = FactorEngine()  # default 100 ms interval
    ev = depth(101, 101, event_time_ms=1234, seq=42)
    assert book.apply_depth(ev).status == "applied"

    snap = engine.on_depth(book, ev)
    assert isinstance(snap, FactorSnapshot)
    assert snap.symbol == SYMBOL
    assert snap.ts_ms == 1234
    assert snap.seq == 42

    # mid = (100 + 101) / 2
    assert snap.mid == pytest.approx(100.5)
    # microprice = (100*2 + 101*3) / 5
    assert snap.microprice == pytest.approx(100.6)
    # spread_bps = (101 - 100) / 100.5 * 1e4
    assert snap.spread_bps == pytest.approx(1.0 / 100.5 * 1e4)
    # top5 depth imbalance = (25 - 20) / (25 + 20)
    assert snap.depth_imbalance_5 == pytest.approx(5.0 / 45.0)
    assert snap.best_bid == pytest.approx(100.0)
    assert snap.best_ask == pytest.approx(101.0)
    assert snap.bid_qty == pytest.approx(3.0)
    assert snap.ask_qty == pytest.approx(2.0)

    # Empty windows on the first snapshot.
    assert snap.ofi_1s == 0.0
    assert snap.trade_imbalance_1s == 0.0
    assert snap.realized_vol_bps == 0.0
    assert snap.momentum_5s == 0.0

    for name in (
        "mid",
        "microprice",
        "spread_bps",
        "ofi_1s",
        "depth_imbalance_5",
        "trade_imbalance_1s",
        "realized_vol_bps",
        "momentum_5s",
        "best_bid",
        "best_ask",
        "bid_qty",
        "ask_qty",
    ):
        assert math.isfinite(getattr(snap, name)), name


def test_first_synced_depth_emits_immediately() -> None:
    engine = FactorEngine(interval_ms=60_000)
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)
    snap = engine.on_depth(book.book, book.apply(42, seq=7))
    assert snap is not None
    assert snap.ts_ms == 42
    assert snap.seq == 7


# ---------------------------------------------------------------------------
# OFI: all branches
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("prev_quote", "new_quote", "expected"),
    [
        # bid price up -> +new bid qty
        ((100 * T, 10 * T, 105 * T, 10 * T), (101 * T, 7 * T, 105 * T, 10 * T), 7.0),
        # bid price unchanged, qty increases -> +difference
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 12 * T, 105 * T, 10 * T), 2.0),
        # bid price unchanged, qty decreases -> -difference
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 4 * T, 105 * T, 10 * T), -6.0),
        # bid price down -> -previous bid qty
        ((100 * T, 10 * T, 105 * T, 10 * T), (99 * T, 8 * T, 105 * T, 10 * T), -10.0),
        # ask price down -> -new ask qty (new sell-side liquidity)
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 10 * T, 104 * T, 7 * T), -7.0),
        # ask price unchanged, qty increases -> -(difference)
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 10 * T, 105 * T, 13 * T), -3.0),
        # ask price unchanged, qty decreases -> -(negative difference)
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 10 * T, 105 * T, 6 * T), 4.0),
        # ask price up -> +previous ask qty (old ask removed)
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 10 * T, 106 * T, 9 * T), 10.0),
        # completely unchanged -> 0
        ((100 * T, 10 * T, 105 * T, 10 * T), (100 * T, 10 * T, 105 * T, 10 * T), 0.0),
    ],
)
def test_ofi_each_change_case(
    prev_quote: tuple[int, int, int, int],
    new_quote: tuple[int, int, int, int],
    expected: float,
) -> None:
    assert _ofi_after_change(prev_quote, new_quote) == pytest.approx(
        expected, rel=1e-12, abs=1e-15
    )


def test_ofi_combined_bid_up_and_ask_down() -> None:
    """Bid improvement (+7) and lower ask (-8) net to -1."""
    value = _ofi_after_change(
        (100 * T, 10 * T, 105 * T, 10 * T),
        (101 * T, 7 * T, 104 * T, 8 * T),
    )
    assert value == pytest.approx(-1.0)


def test_ofi_window_expiry_boundary() -> None:
    """The sliding window is (t - 1000, t]; the boundary event is kept."""
    engine = FactorEngine(interval_ms=0, ofi_window_ms=1000)
    book = QuoteBook(100 * T, 10 * T, 105 * T, 10 * T)

    assert engine.on_depth(book.book, book.apply(0, seq=1)) is not None
    snap = engine.on_depth(book.book, book.apply(1000, bid_px=101 * T, bid_qty=7 * T, seq=2))
    assert snap is not None
    assert snap.ofi_1s == pytest.approx(7.0)

    # At t=2000 the t=1000 contribution is exactly on the boundary.
    snap = engine.on_depth(book.book, book.apply(2000, seq=3))
    assert snap is not None
    assert snap.ofi_1s == pytest.approx(7.0)

    # One millisecond later it has expired.
    snap = engine.on_depth(book.book, book.apply(2001, seq=4))
    assert snap is not None
    assert snap.ofi_1s == 0.0


# ---------------------------------------------------------------------------
# Trade imbalance
# ---------------------------------------------------------------------------
def test_trade_imbalance_is_buyer_maker_semantics_and_window() -> None:
    engine = FactorEngine(interval_ms=0, trade_window_ms=1000)
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)

    # Buyer not maker -> aggressor bought.  Buyer maker -> aggressor sold.
    engine.on_trade(make_trade(1000, 3 * T, is_buyer_maker=False, seq=1))
    engine.on_trade(make_trade(1200, 2 * T, is_buyer_maker=True, seq=2))

    snap = engine.on_depth(book.book, book.apply(1500, seq=1))
    assert snap is not None
    assert snap.trade_imbalance_1s == pytest.approx((3.0 - 2.0) / 5.0)

    # At t=2200 the t=1000 buy expired; the t=1200 sell is exactly on the
    # boundary and is still included -> imbalance is -1.
    snap = engine.on_depth(book.book, book.apply(2200, seq=2))
    assert snap is not None
    assert snap.trade_imbalance_1s == pytest.approx(-1.0)

    # t=2201 expires the sell as well.
    snap = engine.on_depth(book.book, book.apply(2201, seq=3))
    assert snap is not None
    assert snap.trade_imbalance_1s == 0.0


def test_trade_imbalance_zero_without_trades() -> None:
    engine = FactorEngine(interval_ms=0)
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)
    snap = engine.on_depth(book.book, book.apply(1000, seq=1))
    assert snap is not None
    assert snap.trade_imbalance_1s == 0.0

    # A zero-quantity trade is counted but contributes no volume.
    engine.on_trade(make_trade(1050, 0, seq=2))
    snap = engine.on_depth(book.book, book.apply(1100, seq=3))
    assert snap is not None
    assert snap.trade_imbalance_1s == 0.0
    assert engine.stats()[SYMBOL]["trades"] == 1


# ---------------------------------------------------------------------------
# Momentum and volatility
# ---------------------------------------------------------------------------
def test_momentum_requires_four_seconds_of_history() -> None:
    engine = FactorEngine(interval_ms=0)  # default 5 s momentum window
    book = QuoteBook(99 * T, 5 * T, 101 * T, 5 * T)  # mid = 100

    snap = engine.on_depth(book.book, book.apply(0, seq=1))
    assert snap is not None and snap.momentum_5s == 0.0

    # 3.999 s of history -> still suppressed.
    snap = engine.on_depth(
        book.book,
        book.apply(3999, bid_px=109 * T, ask_px=111 * T, seq=2),  # mid = 110
    )
    assert snap is not None
    assert snap.momentum_5s == 0.0

    # Exactly 4 s -> enabled; the oldest point is mid=100.
    snap = engine.on_depth(book.book, book.apply(4000, seq=3))
    assert snap is not None
    assert snap.momentum_5s == pytest.approx(110.0 / 100.0 - 1.0)

    # At t=6000 the t=0 point has left the 5 s window; the oldest remaining
    # point (t=3999) is only ~2 s old, so momentum is suppressed again.
    snap = engine.on_depth(book.book, book.apply(6000, seq=4))
    assert snap is not None
    assert snap.momentum_5s == 0.0


def test_realized_vol_ewma_formula_and_non_negative() -> None:
    alpha = 0.5
    engine = FactorEngine(interval_ms=0, vol_alpha=alpha)
    book = QuoteBook(99 * T, 5 * T, 101 * T, 5 * T)  # mid = 100

    snap = engine.on_depth(book.book, book.apply(0, seq=1))
    assert snap is not None
    assert snap.realized_vol_bps == 0.0  # no previous mid

    # mid 100 -> 110
    snap = engine.on_depth(
        book.book,
        book.apply(100, bid_px=109 * T, ask_px=111 * T, seq=2),
    )
    assert snap is not None
    r1 = math.log(110.0 / 100.0)
    var1 = (1.0 - alpha) * r1 * r1
    assert snap.realized_vol_bps == pytest.approx(math.sqrt(var1) * 1e4)
    assert snap.realized_vol_bps >= 0.0

    # mid 110 -> 100
    snap = engine.on_depth(
        book.book,
        book.apply(200, bid_px=99 * T, ask_px=101 * T, seq=3),
    )
    assert snap is not None
    r2 = math.log(100.0 / 110.0)
    var2 = alpha * var1 + (1.0 - alpha) * r2 * r2
    assert snap.realized_vol_bps == pytest.approx(math.sqrt(var2) * 1e4)
    assert snap.realized_vol_bps >= 0.0

    # A zero return decays the EWMA, so volatility strictly decreases.
    previous = snap.realized_vol_bps
    snap = engine.on_depth(book.book, book.apply(300, seq=4))
    assert snap is not None
    assert snap.realized_vol_bps == pytest.approx(math.sqrt(alpha * var2) * 1e4)
    assert snap.realized_vol_bps < previous

    # Repeated identical mids drive it towards (but never below) zero.
    for i in range(20):
        snap = engine.on_depth(book.book, book.apply(400 + i, seq=5 + i))
        assert snap is not None
        assert snap.realized_vol_bps >= 0.0
        assert math.isfinite(snap.realized_vol_bps)


# ---------------------------------------------------------------------------
# Throttling and out-of-order event time
# ---------------------------------------------------------------------------
def test_throttle_and_out_of_order_timestamps_never_move_backwards() -> None:
    engine = FactorEngine(interval_ms=100)
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)

    # Mix of on-time, early (throttled) and out-of-order timestamps.
    times = [1000, 1050, 1099, 1100, 900, 1150, 1200, 500, 1199, 1300]
    emitted: list[int] = []
    for i, ts in enumerate(times):
        snap = engine.on_depth(book.book, book.apply(ts, seq=i + 1))
        if snap is not None:
            emitted.append(snap.ts_ms)

    assert emitted == [1000, 1100, 1200, 1300]
    assert emitted == sorted(emitted)
    assert engine.stats()[SYMBOL]["last_ts_ms"] == 1300

    # A burst with the same event time never emits a second snapshot.
    assert engine.on_depth(book.book, book.apply(1300, seq=100)) is None
    assert engine.on_depth(book.book, book.apply(1300, seq=101)) is None


def test_windows_keep_updating_while_throttled() -> None:
    """Throttled depth events must still feed the rolling OFI window."""
    engine = FactorEngine(interval_ms=100, ofi_window_ms=1000)
    book = QuoteBook(100 * T, 10 * T, 105 * T, 10 * T)

    assert engine.on_depth(book.book, book.apply(0, seq=1)) is not None
    # Both of these are inside the throttle interval (no snapshot), but their
    # OFI deltas must still accumulate: +7 for the price move, +2 for qty.
    assert (
        engine.on_depth(
            book.book, book.apply(10, bid_px=101 * T, bid_qty=7 * T, seq=2)
        )
        is None
    )
    assert engine.on_depth(book.book, book.apply(20, bid_qty=9 * T, seq=3)) is None

    snap = engine.on_depth(book.book, book.apply(100, seq=4))
    assert snap is not None
    assert snap.ts_ms == 100
    assert snap.ofi_1s == pytest.approx(9.0)


def test_trade_clock_does_not_disturb_depth_throttle() -> None:
    """The depth throttle is defined on depth event time only."""
    engine = FactorEngine(interval_ms=100)
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)

    first = engine.on_depth(book.book, book.apply(1000, seq=1))
    assert first is not None and first.ts_ms == 1000

    # A trade whose event clock runs ahead must not open the depth throttle.
    engine.on_trade(make_trade(5000, 1 * T, seq=2))
    assert engine.on_depth(book.book, book.apply(1050, seq=3)) is None

    snap = engine.on_depth(book.book, book.apply(1100, seq=4))
    assert snap is not None
    assert snap.ts_ms == 1100
    # stats reports the later of the two independently tracked clocks.
    assert engine.stats()[SYMBOL]["last_ts_ms"] == 5000


# ---------------------------------------------------------------------------
# Reset semantics
# ---------------------------------------------------------------------------
def test_reset_book_state_clears_book_state_but_keeps_trades() -> None:
    engine = FactorEngine(interval_ms=1000)
    book = QuoteBook(100 * T, 5 * T, 105 * T, 5 * T)

    first = engine.on_depth(book.book, book.apply(1000, seq=1))
    assert first is not None and first.ts_ms == 1000
    engine.on_trade(make_trade(1000, 2 * T, is_buyer_maker=False, seq=1))

    # Quote improves but the 1 s throttle suppresses this update.
    assert (
        engine.on_depth(
            book.book,
            book.apply(1500, bid_px=101 * T, bid_qty=7 * T, seq=2),
        )
        is None
    )

    engine.reset_book_state(SYMBOL)

    # First update after reset is emitted immediately and starts clean:
    # no OFI against the pre-reset quote, no momentum history, no vol state.
    snap = engine.on_depth(book.book, book.apply(1600, seq=3))
    assert snap is not None
    assert snap.ts_ms == 1600
    assert snap.ofi_1s == 0.0
    assert snap.momentum_5s == 0.0
    assert snap.realized_vol_bps == 0.0
    # Trade window survives the reset: the buy from t=1000 is still inside
    # the (1600 - 1000, 1600] window.
    assert snap.trade_imbalance_1s == pytest.approx(1.0)

    stats = engine.stats()[SYMBOL]
    assert stats["depth_updates"] == 3
    assert stats["snapshots"] == 2
    assert stats["trades"] == 1

    # Resetting an unknown symbol is a no-op.
    engine.reset_book_state("NOSUCHUSDT")
    assert "NOSUCHUSDT" not in engine.stats()


# ---------------------------------------------------------------------------
# Degenerate books
# ---------------------------------------------------------------------------
def test_missing_side_book_returns_none_then_recovers() -> None:
    book = OrderBook(SYMBOL)
    assert book.apply_snapshot(snapshot(1, ((100 * T, 5 * T),), ())).synced
    assert book.synced and book.best() is None  # synced but one-sided

    engine = FactorEngine(interval_ms=0)

    ev1 = depth(2, 2, event_time_ms=1000, seq=1)
    assert book.apply_depth(ev1).status == "applied"
    assert book.best() is None
    assert engine.on_depth(book, ev1) is None

    # The ask reappears: the first usable quote emits and must not fabricate
    # OFI against a quote from before the gap.
    ev2 = depth(3, 3, asks=((101 * T, 2 * T),), event_time_ms=1100, seq=2)
    assert book.apply_depth(ev2).status == "applied"
    snap = engine.on_depth(book, ev2)
    assert snap is not None
    assert snap.ts_ms == 1100
    assert snap.best_bid == pytest.approx(100.0)
    assert snap.best_ask == pytest.approx(101.0)
    assert snap.ofi_1s == 0.0

    stats = engine.stats()[SYMBOL]
    assert stats["depth_updates"] == 2
    assert stats["snapshots"] == 1


def test_unsynchronised_book_returns_none() -> None:
    book = OrderBook(SYMBOL)
    engine = FactorEngine(interval_ms=0)

    ev1 = depth(
        1,
        1,
        ((100 * T, 5 * T),),
        ((101 * T, 5 * T),),
        event_time_ms=1000,
        seq=1,
    )
    assert engine.on_depth(book, ev1) is None
    assert engine.stats()[SYMBOL]["depth_updates"] == 1
    assert engine.stats()[SYMBOL]["snapshots"] == 0

    # Once the book becomes synchronised through a snapshot, factors start.
    assert book.apply_snapshot(
        snapshot(1, ((100 * T, 5 * T),), ((101 * T, 5 * T),))
    ).synced
    ev2 = depth(2, 2, event_time_ms=1100, seq=2)
    assert book.apply_depth(ev2).status == "applied"
    snap = engine.on_depth(book, ev2)
    assert snap is not None
    assert snap.ts_ms == 1100


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def test_stats_shape_counters_and_copy() -> None:
    engine = FactorEngine(interval_ms=0)
    assert engine.stats() == {}

    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)
    engine.on_trade(make_trade(900, 1 * T, seq=1))
    engine.on_depth(book.book, book.apply(1000, seq=2))
    engine.on_depth(book.book, book.apply(1001, seq=3))

    stats = engine.stats()
    assert list(stats) == [SYMBOL]
    entry = stats[SYMBOL]
    assert set(entry) == {"depth_updates", "trades", "snapshots", "last_ts_ms"}
    assert entry["depth_updates"] == 2
    assert entry["trades"] == 1
    assert entry["snapshots"] == 2
    assert entry["last_ts_ms"] == 1001

    # Mutating the returned mapping must not affect engine state.
    entry["depth_updates"] = 999
    assert engine.stats()[SYMBOL]["depth_updates"] == 2

    # Symbols are handled independently.
    other = "ETHUSDT"
    other_book = OrderBook(other)
    assert other_book.apply_snapshot(
        snapshot(1, ((2000 * T, 1 * T),), ((2001 * T, 1 * T),), symbol=other)
    ).synced
    other_ev = depth(2, 2, event_time_ms=50, symbol=other, seq=4)
    assert other_book.apply_depth(other_ev).status == "applied"
    assert engine.on_depth(other_book, other_ev) is not None
    assert engine.stats()[SYMBOL]["depth_updates"] == 2
    assert engine.stats()[other]["depth_updates"] == 1


# ---------------------------------------------------------------------------
# Bounded memory under load
# ---------------------------------------------------------------------------
def test_10k_event_windows_stay_bounded() -> None:
    """10k depth events at 10 ms spacing must not grow windows without bound."""
    engine = FactorEngine(
        interval_ms=100,
        ofi_window_ms=1000,
        trade_window_ms=1000,
        momentum_window_ms=5000,
    )
    book = QuoteBook(100 * T, 5 * T, 101 * T, 5 * T)
    step = T // 100  # 0.01 price unit

    snapshots = 0
    trades = 0
    last_snap: FactorSnapshot | None = None

    for i in range(10_000):
        ts = i * 10
        bid_px = 100 * T + (i % 20) * step
        ask_px = bid_px + T
        ev = book.apply(
            ts,
            bid_px=bid_px,
            bid_qty=(5 + i % 7) * T,
            ask_px=ask_px,
            ask_qty=(4 + i % 5) * T,
            seq=i + 1,
        )
        snap = engine.on_depth(book.book, ev)
        if snap is not None:
            snapshots += 1
            last_snap = snap
        if i % 50 == 0:
            engine.on_trade(
                make_trade(
                    ts,
                    (1 + i % 3) * T,
                    is_buyer_maker=(i // 50) % 2 == 1,
                    seq=20_000 + i,
                )
            )
            trades += 1

    # 10 ms events with a 100 ms interval -> one snapshot per 10 events.
    assert snapshots == 1000
    assert trades == 200

    # White-box window bounds: 1 s / 10 ms = 101 entries, 5 s = 501 entries.
    state = engine._states[SYMBOL]  # noqa: SLF001 - intentional bound check
    assert len(state.ofi_events) <= 110
    assert len(state.mids) <= 510
    assert len(state.trade_events) <= 10

    stats = engine.stats()[SYMBOL]
    assert stats["depth_updates"] == 10_000
    assert stats["trades"] == 200
    assert stats["snapshots"] == 1000
    assert stats["last_ts_ms"] == 99_990

    assert last_snap is not None
    assert math.isfinite(last_snap.realized_vol_bps)
    assert last_snap.realized_vol_bps >= 0.0


# ---------------------------------------------------------------------------
# Seeded property-style checks
# ---------------------------------------------------------------------------
def _run_random_stream(seed: int) -> list[dict]:
    """Run a deterministic pseudo-random stream and collect snapshots."""
    rng = random.Random(seed)
    engine = FactorEngine(
        interval_ms=17,
        ofi_window_ms=300,
        trade_window_ms=400,
        momentum_window_ms=5000,
        vol_alpha=0.9,
    )
    book = QuoteBook(100 * T, 10 * T, 101 * T, 10 * T)

    out: list[dict] = []
    ts = 0
    bid_px = 100 * T
    for i in range(600):
        ts += rng.randint(0, 20)
        bid_px += rng.choice((-2, -1, 0, 1, 2)) * (T // 100)
        bid_px = min(max(bid_px, 90 * T), 99 * T)
        ask_px = bid_px + T
        ev = book.apply(
            ts,
            bid_px=bid_px,
            bid_qty=rng.randint(1, 20) * T,
            ask_px=ask_px,
            ask_qty=rng.randint(1, 20) * T,
            seq=i + 1,
        )
        snap = engine.on_depth(book.book, ev)
        if rng.random() < 0.25:
            engine.on_trade(
                make_trade(
                    ts,
                    rng.randint(1, 5) * T,
                    is_buyer_maker=rng.random() < 0.5,
                    seq=100_000 + i,
                )
            )
        if snap is not None:
            out.append(snap.to_dict())
    return out


def test_random_stream_outputs_are_finite_and_deterministic() -> None:
    first = _run_random_stream(7)
    second = _run_random_stream(7)

    assert len(first) > 100  # sanity: the stream actually produced snapshots
    # Identical inputs must produce byte-identical derived records.
    assert first == second

    float_fields = (
        "mid",
        "microprice",
        "spread_bps",
        "ofi_1s",
        "depth_imbalance_5",
        "trade_imbalance_1s",
        "realized_vol_bps",
        "momentum_5s",
        "best_bid",
        "best_ask",
        "bid_qty",
        "ask_qty",
    )
    previous_ts = -1
    for record in first:
        for name in float_fields:
            value = record[name]
            assert isinstance(value, float)
            assert math.isfinite(value), (name, value)

        assert record["mid"] > 0.0
        assert record["spread_bps"] >= 0.0
        assert -1.0 <= record["depth_imbalance_5"] <= 1.0
        assert -1.0 <= record["trade_imbalance_1s"] <= 1.0
        assert record["realized_vol_bps"] >= 0.0
        assert record["best_bid"] < record["best_ask"]
        assert record["bid_qty"] > 0.0
        assert record["ask_qty"] > 0.0
        assert record["ts_ms"] >= previous_ts
        previous_ts = record["ts_ms"]
