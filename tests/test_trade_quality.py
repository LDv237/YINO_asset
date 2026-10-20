"""Tests for per-symbol aggTrade ID continuity tracking."""

from __future__ import annotations

from tickforge.events import TradeEvent
from tickforge.runtime.trade_quality import TradeQualityTracker


def _trade(symbol: str, trade_id: int) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        event_time_ms=trade_id,
        agg_trade_id=trade_id,
        price_ticks=100_000_000,
        qty_ticks=1_000_000,
        is_buyer_maker=False,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=trade_id,
    )


def test_contiguous_duplicate_backwards_and_gap() -> None:
    tracker = TradeQualityTracker()
    tracker.ensure("BTCUSDT")
    assert tracker.observe(_trade("BTCUSDT", 100)) == ("first", 0)
    assert tracker.observe(_trade("BTCUSDT", 101)) == ("ok", 0)
    assert tracker.observe(_trade("BTCUSDT", 101)) == ("duplicate", 0)
    assert tracker.observe(_trade("BTCUSDT", 99)) == ("backwards", 0)
    assert tracker.observe(_trade("BTCUSDT", 104)) == ("gap", 2)
    assert tracker.observe(_trade("BTCUSDT", 105)) == ("ok", 0)

    stats = tracker.stats()["BTCUSDT"]
    assert stats["last_agg_trade_id"] == 105
    assert stats["accepted"] == 4  # 100, 101, 104, 105
    assert stats["duplicates"] == 1
    assert stats["backwards"] == 1
    assert stats["gaps"] == 1
    assert stats["missing_ids"] == 2


def test_symbols_are_independent_and_empty_symbols_are_reported() -> None:
    tracker = TradeQualityTracker()
    tracker.ensure("BTCUSDT")
    tracker.ensure("ETHUSDT")
    tracker.observe(_trade("BTCUSDT", 50))
    tracker.observe(_trade("ETHUSDT", 9_000_000))
    tracker.observe(_trade("BTCUSDT", 52))
    stats = tracker.stats()
    assert set(stats) == {"BTCUSDT", "ETHUSDT"}
    assert stats["BTCUSDT"]["gaps"] == 1
    assert stats["ETHUSDT"]["gaps"] == 0
    assert stats["ETHUSDT"]["last_agg_trade_id"] == 9_000_000
