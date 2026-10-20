"""Tests for the Binance-synchronised L2 order book.

The suite covers the documented Binance synchronisation example, sequencing
edge cases (duplicates, overlaps, gaps, stale snapshots, resync), book
integrity (zero-quantity deletion, lazy heap pruning, crossed books, top-N
ordering), bounded buffering, and a randomized differential check against a
naive full-depth book implemented independently from the same events.
"""

from __future__ import annotations

import random
from decimal import Decimal

import pytest

from tickforge.book import BestQuote, BookResult, OrderBook
from tickforge.events import DepthEvent, SnapshotEvent

SYMBOL = "BTCUSDT"
TICKS = 100_000_000


def px(value: str) -> int:
    """Decimal price string -> integer ticks."""
    return int(Decimal(value) * TICKS)


def qty(value: str) -> int:
    """Decimal quantity string -> integer ticks."""
    return int(Decimal(value) * TICKS)


def lvl(price: str, quantity: str) -> tuple[int, int]:
    return px(price), qty(quantity)


def snapshot(
    last_update_id: int,
    bids: tuple[tuple[int, int], ...] | list[tuple[int, int]] = (),
    asks: tuple[tuple[int, int], ...] | list[tuple[int, int]] = (),
    *,
    symbol: str = SYMBOL,
    seq: int = 0,
) -> SnapshotEvent:
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
    bids: tuple[tuple[int, int], ...] | list[tuple[int, int]] = (),
    asks: tuple[tuple[int, int], ...] | list[tuple[int, int]] = (),
    *,
    symbol: str = SYMBOL,
    seq: int = 0,
    event_time_ms: int = 0,
) -> DepthEvent:
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


def synced_book(
    last: int = 100,
    bids: list[tuple[int, int]] | None = None,
    asks: list[tuple[int, int]] | None = None,
) -> OrderBook:
    book = OrderBook(SYMBOL)
    bids = bids if bids is not None else [lvl("100.0", "5"), lvl("99.0", "7")]
    asks = asks if asks is not None else [lvl("101.0", "4"), lvl("102.0", "8")]
    result = book.apply_snapshot(snapshot(last, bids, asks))
    assert result.status == "applied"
    return book


# ---------------------------------------------------------------------------
# Initial state, validation
# ---------------------------------------------------------------------------
def test_initial_state_and_buffering() -> None:
    book = OrderBook(SYMBOL)

    assert book.synced is False
    assert book.needs_snapshot is True
    assert book.last_update_id == 0
    assert book.best() is None
    assert book.mid() is None
    assert book.microprice() is None
    assert book.depth_imbalance() == 0.0
    assert book.top_levels() == ([], [])

    result = book.apply_depth(depth(1, 5, bids=[lvl("100.0", "2")]))
    assert isinstance(result, BookResult)
    assert result.status == "buffered"
    assert result.synced is False
    assert result.needs_snapshot is True
    assert result.best_changed is False
    assert result.buffered == 1
    assert book.stats()["buffered"] == 1


def test_constructor_validation() -> None:
    with pytest.raises(ValueError):
        OrderBook("")
    with pytest.raises(ValueError):
        OrderBook(SYMBOL, max_buffer=0)
    with pytest.raises(ValueError):
        OrderBook(SYMBOL, max_buffer=-3)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Binance documented flow
# ---------------------------------------------------------------------------
def test_binance_documented_snapshot_and_first_event() -> None:
    """Snapshot lastUpdateId=160, stale event 157..160, first valid 161..165."""
    book = OrderBook(SYMBOL)

    # Diff events are received while the REST snapshot is in flight.
    r1 = book.apply_depth(
        depth(157, 160, bids=[lvl("0.0024", "10")], asks=[lvl("0.0026", "100")])
    )
    r2 = book.apply_depth(
        depth(161, 165, bids=[lvl("0.0024", "12"), lvl("0.0025", "20")])
    )
    assert r1.status == "buffered" and r1.buffered == 1
    assert r2.status == "buffered" and r2.buffered == 2

    result = book.apply_snapshot(
        snapshot(160, bids=[lvl("0.0024", "10")], asks=[lvl("0.0026", "100")])
    )

    assert result.status == "applied"
    assert result.synced is True
    assert result.needs_snapshot is False
    assert result.best_changed is True
    assert result.buffered == 0
    assert book.synced is True
    assert book.last_update_id == 165

    # Event 157..160 is covered by the snapshot and must be dropped.
    stats = book.stats()
    assert stats["duplicates"] == 1
    assert stats["applied"] == 1
    assert stats["snapshots"] == 1

    quote = book.best()
    assert quote == BestQuote(
        bid_px=px("0.0025"),
        bid_qty=qty("20"),
        ask_px=px("0.0026"),
        ask_qty=qty("100"),
    )
    bids, asks = book.top_levels(5)
    assert bids == [(px("0.0025"), qty("20")), (px("0.0024"), qty("12"))]
    assert asks == [(px("0.0026"), qty("100"))]


# ---------------------------------------------------------------------------
# Continuous updates, duplicates, overlaps
# ---------------------------------------------------------------------------
def test_continuous_updates_and_best_changed() -> None:
    book = synced_book(100)
    initial = book.best()
    assert initial is not None and initial.bid_qty == qty("5")

    r = book.apply_depth(depth(101, 101, bids=[lvl("100.0", "6")]))
    assert r.status == "applied"
    assert r.best_changed is True
    assert book.last_update_id == 101
    assert book.best() == BestQuote(px("100.0"), qty("6"), px("101.0"), qty("4"))

    r = book.apply_depth(depth(102, 102, bids=[], asks=[]))
    assert r.status == "applied"
    assert r.best_changed is False
    assert book.last_update_id == 102

    r = book.apply_depth(depth(103, 105, bids=[lvl("99.0", "0")]))
    assert r.status == "applied"
    assert r.best_changed is False  # deleted level was not the best bid
    assert book.last_update_id == 105
    assert book.stats()["bids"] == 1

    assert book.stats()["applied"] == 3


def test_duplicate_event_is_ignored() -> None:
    book = synced_book(100)
    before = book.best()

    result = book.apply_depth(depth(98, 100, bids=[lvl("100.0", "99")]))

    assert result.status == "duplicate"
    assert result.synced is True
    assert result.best_changed is False
    assert result.buffered == 0
    assert book.last_update_id == 100
    assert book.best() == before
    assert book.stats()["duplicates"] == 1


def test_overlapping_events_are_allowed() -> None:
    book = synced_book(100)

    # U <= last + 1 <= u: an event may start before the current id.
    r1 = book.apply_depth(depth(99, 105, bids=[lvl("100.0", "8")]))
    assert r1.status == "applied"
    assert book.last_update_id == 105

    # Next event may also overlap the previous one.
    r2 = book.apply_depth(depth(105, 110, asks=[lvl("101.0", "3")]))
    assert r2.status == "applied"
    assert book.last_update_id == 110
    assert book.best() == BestQuote(px("100.0"), qty("8"), px("101.0"), qty("3"))

    # Fully covered event afterwards is a duplicate.
    r3 = book.apply_depth(depth(90, 110, bids=[lvl("99.0", "1")]))
    assert r3.status == "duplicate"
    assert book.stats()["applied"] == 2
    assert book.stats()["duplicates"] == 1


# ---------------------------------------------------------------------------
# Gap and resynchronisation
# ---------------------------------------------------------------------------
def test_gap_buffers_then_recovers_with_new_snapshot() -> None:
    book = synced_book(100)

    # 110 > last + 1 => missing updates.
    r = book.apply_depth(depth(110, 115, bids=[lvl("100.0", "6")]))
    assert r.status == "gap"
    assert r.synced is False
    assert r.needs_snapshot is True
    assert r.best_changed is True
    assert r.buffered == 1
    assert book.best() is None
    assert book.last_update_id == 100

    r = book.apply_depth(depth(116, 120, asks=[lvl("101.0", "7")]))
    assert r.status == "buffered"
    assert r.buffered == 2

    # The snapshot's lastUpdateId (112) falls inside the first buffered event.
    result = book.apply_snapshot(
        snapshot(112, bids=[lvl("100.0", "5")], asks=[lvl("101.0", "4")])
    )
    assert result.status == "applied"
    assert result.synced is True
    assert book.last_update_id == 120
    assert book.best() == BestQuote(px("100.0"), qty("6"), px("101.0"), qty("7"))
    assert book.stats()["gaps"] == 1
    assert book.stats()["buffered"] == 0


def test_stale_snapshot_keeps_buffer_and_stays_waiting() -> None:
    book = OrderBook(SYMBOL)
    book.apply_depth(depth(200, 205, bids=[lvl("100.0", "1")]))
    book.apply_depth(depth(206, 210, bids=[lvl("100.0", "2")]))

    # 200 > 150 + 1: the snapshot predates the oldest buffered event.
    result = book.apply_snapshot(snapshot(150, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")]))
    assert result.status == "buffered"
    assert result.synced is False
    assert result.needs_snapshot is True
    assert result.buffered == 2
    assert book.best() is None
    assert book.stats()["buffered"] == 2  # buffer kept intact

    book.apply_depth(depth(211, 215, bids=[lvl("100.0", "3")]))
    assert book.stats()["buffered"] == 3

    # A snapshot whose lastUpdateId is exactly U-1 makes the stream contiguous.
    result = book.apply_snapshot(snapshot(199, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")]))
    assert result.status == "applied"
    assert result.synced is True
    assert book.last_update_id == 215
    assert book.stats()["buffered"] == 0
    assert book.stats()["applied"] == 3


def test_snapshot_replay_detects_gap() -> None:
    book = OrderBook(SYMBOL)
    book.apply_depth(depth(1, 2, bids=[lvl("100.0", "1")]))
    book.apply_depth(depth(5, 6, bids=[lvl("100.0", "2")]))  # gap vs 1..2
    book.apply_depth(depth(7, 8, bids=[lvl("100.0", "3")]))

    result = book.apply_snapshot(snapshot(1, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")]))
    assert result.status == "gap"
    assert result.synced is False
    assert result.needs_snapshot is True
    assert result.buffered == 2
    assert book.last_update_id == 2
    assert book.best() is None  # stale/partial book is hidden
    assert book.stats()["gaps"] == 1

    result = book.apply_snapshot(snapshot(4, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")]))
    assert result.status == "applied"
    assert result.synced is True
    assert book.last_update_id == 8
    assert book.stats()["applied"] == 3  # one replay event from each snapshot


def test_begin_resync_clears_everything() -> None:
    book = synced_book(100)
    book.begin_resync()

    assert book.synced is False
    assert book.needs_snapshot is True
    assert book.last_update_id == 0
    assert book.best() is None
    assert book.mid() is None
    assert book.microprice() is None
    assert book.depth_imbalance() == 0.0
    assert book.top_levels() == ([], [])
    stats = book.stats()
    assert stats["bids"] == 0
    assert stats["asks"] == 0
    assert stats["buffered"] == 0

    # Events received after a resync are buffered for the next snapshot.
    r = book.apply_depth(depth(1, 2, bids=[lvl("100.0", "1")]))
    assert r.status == "buffered"
    assert book.stats()["buffered"] == 1

    result = book.apply_snapshot(snapshot(0, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")]))
    assert result.status == "applied"
    assert book.synced is True


# ---------------------------------------------------------------------------
# Crossed books
# ---------------------------------------------------------------------------
def test_crossed_book_invalidates_sync() -> None:
    book = synced_book(100)
    assert book.best() is not None

    # An ask at the current best bid price crosses the book.
    r = book.apply_depth(depth(101, 101, asks=[lvl("100.0", "3")]))
    assert r.status == "crossed"
    assert r.synced is False
    assert r.needs_snapshot is True
    assert r.best_changed is True
    assert r.buffered == 1
    assert book.best() is None
    assert book.top_levels() == ([], [])
    assert book.mid() is None
    assert book.depth_imbalance() == 0.0
    assert book.stats()["crossed"] == 1

    # Later diffs are buffered until a snapshot arrives.
    r = book.apply_depth(depth(102, 102, bids=[lvl("100.0", "6")]))
    assert r.status == "buffered"
    assert r.buffered == 2

    # Snapshot covering the crossing event; the remaining diff is replayed.
    result = book.apply_snapshot(
        snapshot(101, bids=[lvl("100.0", "5")], asks=[lvl("101.0", "4")])
    )
    assert result.status == "applied"
    assert result.synced is True
    assert book.last_update_id == 102
    assert book.best() == BestQuote(px("100.0"), qty("6"), px("101.0"), qty("4"))
    assert book.stats()["crossed"] == 1


def test_crossed_book_during_snapshot_replay() -> None:
    book = OrderBook(SYMBOL)
    book.apply_depth(depth(1, 5, asks=[lvl("200.0", "2")]))
    book.apply_depth(depth(6, 10, asks=[lvl("100.0", "2")]))  # crosses bid 150

    result = book.apply_snapshot(
        snapshot(0, bids=[lvl("150.0", "5")], asks=[lvl("200.0", "5")])
    )
    assert result.status == "crossed"
    assert result.synced is False
    assert result.needs_snapshot is True
    assert result.buffered == 1  # the crossing event is retained
    assert book.best() is None
    assert book.stats()["crossed"] == 1


def test_snapshot_itself_crossed_is_rejected() -> None:
    book = OrderBook(SYMBOL)
    result = book.apply_snapshot(
        snapshot(10, bids=[lvl("101.0", "1")], asks=[lvl("100.0", "1")])
    )
    assert result.status == "crossed"
    assert result.synced is False
    assert result.needs_snapshot is True
    assert book.best() is None
    assert book.stats()["crossed"] == 1


# ---------------------------------------------------------------------------
# Level handling and top-N
# ---------------------------------------------------------------------------
def test_zero_qty_deletes_levels_and_heap_resurrection() -> None:
    book = synced_book(100)
    assert book.stats()["bids"] == 2

    r = book.apply_depth(depth(101, 101, bids=[lvl("100.0", "0")]))
    assert r.status == "applied"
    assert book.best() == BestQuote(px("99.0"), qty("7"), px("101.0"), qty("4"))
    bids, _ = book.top_levels(5)
    assert bids == [(px("99.0"), qty("7"))]
    assert book.stats()["bids"] == 1

    r = book.apply_depth(depth(102, 102, bids=[lvl("99.0", "0")]))
    assert r.status == "applied"
    assert book.best() is None  # one-sided book exposes no BestQuote
    bids, asks = book.top_levels(5)
    assert bids == []
    assert asks == [(px("101.0"), qty("4")), (px("102.0"), qty("8"))]
    assert book.stats()["bids"] == 0

    # Re-adding a price that was pruned from the heap must still work.
    r = book.apply_depth(depth(103, 103, bids=[lvl("100.0", "3")]))
    assert r.status == "applied"
    assert book.best() == BestQuote(px("100.0"), qty("3"), px("101.0"), qty("4"))
    bids, _ = book.top_levels(5)
    assert bids == [(px("100.0"), qty("3"))]


def test_top_levels_ordering_and_limits() -> None:
    bids = [lvl("96.0", "1"), lvl("100.0", "2"), lvl("98.0", "3"), lvl("97.0", "4"), lvl("99.0", "5")]
    asks = [lvl("105.0", "6"), lvl("101.0", "7"), lvl("103.0", "8"), lvl("102.0", "9"), lvl("104.0", "10")]
    book = synced_book(100, bids, asks)

    top_bids, top_asks = book.top_levels(3)
    assert top_bids == [(px("100.0"), qty("2")), (px("99.0"), qty("5")), (px("98.0"), qty("3"))]
    assert top_asks == [(px("101.0"), qty("7")), (px("102.0"), qty("9")), (px("103.0"), qty("8"))]

    full_bids, full_asks = book.top_levels(100)
    assert full_bids == sorted(full_bids, key=lambda level: -level[0])
    assert full_asks == sorted(full_asks, key=lambda level: level[0])
    assert len(full_bids) == 5 and len(full_asks) == 5
    assert full_bids[0][0] == px("100.0") and full_asks[0][0] == px("101.0")

    assert book.top_levels(0) == ([], [])
    assert book.top_levels(-2) == ([], [])


# ---------------------------------------------------------------------------
# Buffer overflow
# ---------------------------------------------------------------------------
def test_buffer_overflow_drops_oldest() -> None:
    book = OrderBook(SYMBOL, max_buffer=3)
    for k in range(5):
        r = book.apply_depth(depth(2 * k + 1, 2 * k + 2, bids=[(100 + k, 1)]))
        assert r.status == "buffered"

    stats = book.stats()
    assert stats["buffered"] == 3
    assert stats["buffer_overflow"] == 2
    assert book.needs_snapshot is True

    # The retained events are U=5..6, 7..8, 9..10 (the two oldest were dropped).
    result = book.apply_snapshot(snapshot(4, bids=[(100, 1)], asks=[(200, 1)]))
    assert result.status == "applied"
    assert result.synced is True
    assert book.last_update_id == 10
    assert book.stats()["applied"] == 3
    assert book.stats()["buffered"] == 0


# ---------------------------------------------------------------------------
# Symbol routing and read-only accessors
# ---------------------------------------------------------------------------
def test_wrong_symbol_is_ignored() -> None:
    book = synced_book(100)
    before = book.best()
    applied_before = book.stats()["applied"]
    snapshots_before = book.stats()["snapshots"]

    r = book.apply_depth(depth(101, 101, bids=[lvl("100.0", "1")], symbol="ETHUSDT"))
    assert r.status == "ignored"
    assert book.best() == before
    assert book.stats()["applied"] == applied_before
    assert book.last_update_id == 100

    r = book.apply_snapshot(snapshot(500, bids=[lvl("1.0", "1")], asks=[lvl("2.0", "1")], symbol="ETHUSDT"))
    assert r.status == "ignored"
    assert book.best() == before
    assert book.stats()["snapshots"] == snapshots_before


def test_mid_microprice_and_imbalance_hand_computed() -> None:
    book = synced_book(
        1,
        bids=[lvl("100.0", "2"), lvl("99.0", "4")],
        asks=[lvl("101.0", "3"), lvl("102.0", "1")],
    )

    assert book.mid() == pytest.approx(100.5)
    # (100*3 + 101*2) / (2 + 3) = 100.4
    assert book.microprice() == pytest.approx(100.4)
    assert book.depth_imbalance(1) == pytest.approx(-0.2)
    assert book.depth_imbalance(2) == pytest.approx((6 - 4) / (6 + 4))
    assert book.depth_imbalance(0) == 0.0
    assert -1.0 <= book.depth_imbalance() <= 1.0


def test_empty_depth_event_advances_sequence() -> None:
    book = synced_book(100)
    before = book.best()

    r = book.apply_depth(depth(101, 101))
    assert r.status == "applied"
    assert r.best_changed is False
    assert book.last_update_id == 101
    assert book.best() == before


def test_stats_schema_has_exact_required_fields() -> None:
    expected = {
        "symbol",
        "synced",
        "last_update_id",
        "bids",
        "asks",
        "buffered",
        "applied",
        "duplicates",
        "gaps",
        "snapshots",
        "crossed",
        "buffer_overflow",
        "stale_snapshots",
    }
    book = synced_book(100)
    stats = book.stats()
    assert set(stats) == expected
    assert stats["symbol"] == SYMBOL
    assert stats["synced"] is True
    assert stats["last_update_id"] == 100
    assert stats["bids"] == 2
    assert stats["asks"] == 2
    assert stats["snapshots"] == 1


def test_snapshot_applied_while_synced_replaces_book() -> None:
    book = synced_book(100)
    result = book.apply_snapshot(
        snapshot(200, bids=[lvl("50.0", "1")], asks=[lvl("60.0", "2")])
    )
    assert result.status == "applied"
    assert result.best_changed is True
    assert book.synced is True
    assert book.last_update_id == 200
    assert book.best() == BestQuote(px("50.0"), qty("1"), px("60.0"), qty("2"))
    assert book.top_levels() == (
        [(px("50.0"), qty("1"))],
        [(px("60.0"), qty("2"))],
    )


def test_snapshot_covering_entire_buffer_becomes_synced() -> None:
    book = OrderBook(SYMBOL)
    book.apply_depth(depth(10, 15, bids=[lvl("100.0", "1")]))
    book.apply_depth(depth(16, 20, bids=[lvl("100.0", "2")]))

    result = book.apply_snapshot(
        snapshot(25, bids=[lvl("100.0", "3")], asks=[lvl("101.0", "4")])
    )
    assert result.status == "applied"
    assert result.synced is True
    assert result.needs_snapshot is False
    assert result.buffered == 0
    assert book.stats()["duplicates"] == 2
    assert book.stats()["applied"] == 0
    assert book.last_update_id == 25
    assert book.best() == BestQuote(px("100.0"), qty("3"), px("101.0"), qty("4"))


def test_heap_compaction_keeps_book_correct() -> None:
    book = synced_book(1, bids=[lvl("100.0", "1")], asks=[lvl("200.0", "1")])
    seq = 1

    # Churn many transient price levels far below the best bid so that stale
    # lazy-deletion heap entries accumulate and trigger a compaction.
    for wave in range(5):
        prices = [px("10.0") + wave * 1_000 + offset for offset in range(100)]
        seq += 1
        assert book.apply_depth(depth(seq, seq, bids=[(p, 7) for p in prices])).status == "applied"
        seq += 1
        assert book.apply_depth(depth(seq, seq, bids=[(p, 0) for p in prices])).status == "applied"

    assert book.best() == BestQuote(px("100.0"), qty("1"), px("200.0"), qty("1"))
    assert book.top_levels(1_000) == (
        [(px("100.0"), qty("1"))],
        [(px("200.0"), qty("1"))],
    )
    assert len(book._bid_heap) <= 2 * len(book._bids) + 64


def test_one_sided_snapshot_is_synced_but_has_no_quote() -> None:
    book = OrderBook(SYMBOL)
    result = book.apply_snapshot(snapshot(10, bids=[lvl("100.0", "1")]))
    assert result.status == "applied"
    assert result.synced is True
    assert book.best() is None
    assert book.mid() is None
    assert book.microprice() is None
    assert book.top_levels() == ([(px("100.0"), qty("1"))], [])

    r = book.apply_depth(depth(11, 11, asks=[lvl("101.0", "2")]))
    assert r.status == "applied"
    assert r.best_changed is True
    assert book.best() == BestQuote(px("100.0"), qty("1"), px("101.0"), qty("2"))


# ---------------------------------------------------------------------------
# Randomized differential test against a naive full book
# ---------------------------------------------------------------------------
class _NaiveBook:
    """Independent full-depth reference implementation (no heaps, no sync)."""

    def __init__(self) -> None:
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}

    def apply(self, bids: list[tuple[int, int]], asks: list[tuple[int, int]]) -> None:
        for price, quantity in bids:
            if quantity <= 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = quantity
        for price, quantity in asks:
            if quantity <= 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = quantity

    def best(self) -> tuple[int, int, int, int] | None:
        if not self.bids or not self.asks:
            return None
        bid = max(self.bids)
        ask = min(self.asks)
        return bid, self.bids[bid], ask, self.asks[ask]

    def levels(self, n: int) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        if n <= 0:
            return [], []
        return (
            sorted(self.bids.items(), key=lambda item: -item[0])[:n],
            sorted(self.asks.items(), key=lambda item: item[0])[:n],
        )

    def mid(self) -> float | None:
        quote = self.best()
        if quote is None:
            return None
        return (quote[0] + quote[2]) / 2 / TICKS

    def microprice(self) -> float | None:
        quote = self.best()
        if quote is None:
            return None
        bid_px, bid_qty, ask_px, ask_qty = quote
        return (bid_px * ask_qty + ask_px * bid_qty) / (bid_qty + ask_qty) / TICKS

    def imbalance(self, n: int) -> float:
        bids, asks = self.levels(n)
        bid_qty = sum(quantity for _, quantity in bids)
        ask_qty = sum(quantity for _, quantity in asks)
        total = bid_qty + ask_qty
        if total <= 0:
            return 0.0
        return (bid_qty - ask_qty) / total


def test_randomized_differential_against_naive_book() -> None:
    rng = random.Random(7)
    bid_pool = list(range(1_000, 1_050))
    ask_pool = list(range(2_000, 2_050))

    for _trial in range(8):
        book = OrderBook(SYMBOL)
        naive = _NaiveBook()

        snap_bids = [(price, rng.randrange(1, 1_000)) for price in rng.sample(bid_pool, 8)]
        snap_asks = [(price, rng.randrange(1, 1_000)) for price in rng.sample(ask_pool, 8)]
        result = book.apply_snapshot(snapshot(1_000, snap_bids, snap_asks))
        naive.apply(snap_bids, snap_asks)
        assert result.status == "applied"
        assert result.synced is True

        last = 1_000
        applied = 0
        duplicates = 0

        for _step in range(120):
            if rng.random() < 0.15:
                # Stale duplicate: u <= last, must not change the book.
                final = max(1, last - rng.randrange(0, 3))
                r = book.apply_depth(
                    depth(final, final, [(rng.choice(bid_pool), rng.randrange(0, 50))], [])
                )
                assert r.status == "duplicate"
                duplicates += 1
                continue

            overlap = rng.randrange(0, 3)
            first = max(1, last + 1 - overlap)
            final = last + 1 + rng.randrange(0, 3)  # strictly greater than last

            upd_bids = [
                (rng.choice(bid_pool), rng.randrange(0, 500))
                for _ in range(rng.randrange(0, 4))
            ]
            upd_asks = [
                (rng.choice(ask_pool), rng.randrange(0, 500))
                for _ in range(rng.randrange(0, 4))
            ]

            r = book.apply_depth(depth(first, final, upd_bids, upd_asks))
            naive.apply(upd_bids, upd_asks)
            applied += 1

            assert r.status == "applied"
            assert r.synced is True
            assert isinstance(r.best_changed, bool)

            assert book.last_update_id == final
            last = final
            quote = book.best()
            naive_quote = naive.best()
            if naive_quote is None:
                assert quote is None
            else:
                assert quote == BestQuote(*naive_quote)

            for n in (1, 3, 5, 1_000):
                bid_levels, ask_levels = book.top_levels(n)
                assert (bid_levels, ask_levels) == naive.levels(n)

            assert book.mid() == pytest.approx(naive.mid())
            assert book.microprice() == pytest.approx(naive.microprice())
            assert book.depth_imbalance(3) == pytest.approx(naive.imbalance(3))
            assert -1.0 <= book.depth_imbalance(5) <= 1.0

        stats = book.stats()
        assert stats["applied"] == applied
        assert stats["duplicates"] == duplicates
        assert stats["buffered"] == 0
        assert stats["synced"] is True
        assert book.top_levels(10_000) == naive.levels(10_000)


def test_stale_snapshot_while_synced_is_ignored_without_rewind() -> None:
    book = OrderBook(SYMBOL)
    result = book.apply_snapshot(
        snapshot(200, [lvl("100", "5")], [lvl("101", "4")])
    )
    assert result.synced is True
    for update_id in range(201, 211):
        result = book.apply_depth(
            depth(update_id, update_id, [lvl("100", str(5 + update_id - 200))])
        )
        assert result.status == "applied"
    assert book.last_update_id == 210
    before = book.best()
    assert before is not None

    # A snapshot fetched before the last ten diffs can arrive late (REST
    # latency plus ingest-lock contention).  It must not rewind the book.
    result = book.apply_snapshot(
        snapshot(205, [lvl("100", "1")], [lvl("101", "1")])
    )
    assert result.status == "ignored"
    assert result.synced is True
    assert result.needs_snapshot is False
    assert book.last_update_id == 210
    assert book.best() == before
    assert book.stats()["stale_snapshots"] == 1

    # The next contiguous diff is still applicable; no spurious gap.
    result = book.apply_depth(depth(211, 211, [lvl("100", "99")]))
    assert result.status == "applied"
    assert book.synced is True
    assert book.stats()["gaps"] == 0
