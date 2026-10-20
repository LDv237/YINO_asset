"""Long-only spot execution variant.

The original abstract simulator allows negative positions.  These tests pin the
optional spot mode: no naked sells, sells capped by inventory, buys capped by
quote cash, and initial cash excluded from PnL.
"""

from __future__ import annotations

import pytest

from tickforge.config import Config, StrategyConfig
from tickforge.events import TICKS_PER_UNIT, FactorSnapshot, Signal, TradeEvent
from tickforge.exec.simulator import Simulator
from tickforge.strategy.mm import MIN_QTY, MarketMaker


def _ticks(value: float) -> int:
    return int(round(value * TICKS_PER_UNIT))


def _signal(
    *,
    bid: float | None = 99.0,
    bid_qty: float = 1.0,
    ask: float | None = 101.0,
    ask_qty: float = 1.0,
    ts_ms: int = 1_000,
) -> Signal:
    return Signal(
        symbol="BTCUSDT",
        ts_ms=ts_ms,
        seq=1,
        fair_price=100.0,
        bid_price=bid,
        bid_qty=bid_qty,
        ask_price=ask,
        ask_qty=ask_qty,
        reason="mm",
    )


def _trade(price: float, qty: float, *, trade_id: int = 1) -> TradeEvent:
    return TradeEvent(
        symbol="BTCUSDT",
        event_time_ms=1_000,
        agg_trade_id=trade_id,
        price_ticks=_ticks(price),
        qty_ticks=_ticks(qty),
        is_buyer_maker=True,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=1,
    )


def _cfg(**overrides) -> StrategyConfig:
    values = dict(
        allow_short=False,
        initial_cash_usd=1_000.0,
        quote_notional_usd=100.0,
        max_position_notional_usd=500.0,
        fee_bps=0.0,
        quote_cooldown_ms=0,
    )
    values.update(overrides)
    return StrategyConfig(**values)


def _snapshot(ts_ms: int) -> FactorSnapshot:
    return FactorSnapshot(
        symbol="BTCUSDT",
        ts_ms=ts_ms,
        seq=ts_ms,
        mid=100.0,
        microprice=100.0,
        spread_bps=1.0,
        ofi_1s=0.0,
        depth_imbalance_5=0.0,
        trade_imbalance_1s=0.0,
        realized_vol_bps=0.0,
        momentum_5s=0.0,
        best_bid=99.99,
        best_ask=100.01,
        bid_qty=1.0,
        ask_qty=1.0,
    )


def test_long_only_rejects_sell_without_inventory() -> None:
    sim = Simulator(_cfg())
    sim.apply_signal(_signal())
    fills = sim.on_trade(_trade(101.0, 1.0))
    assert fills == []
    assert sim.position("BTCUSDT") == 0.0
    assert sim.summary()["symbols"]["BTCUSDT"]["risk_rejected"] >= 1


def test_long_only_buy_then_sell_never_goes_negative() -> None:
    sim = Simulator(_cfg())
    sim.apply_signal(_signal())
    buys = sim.on_trade(_trade(99.0, 1.0, trade_id=1))
    assert len(buys) == 1
    assert sim.position("BTCUSDT") == pytest.approx(1.0)
    assert sim.summary()["symbols"]["BTCUSDT"]["cash"] == pytest.approx(901.0)

    sim.apply_signal(_signal(bid=None, ask=101.0, ask_qty=5.0))
    sells = sim.on_trade(_trade(101.0, 2.0, trade_id=2))
    assert len(sells) == 1
    assert sells[0].qty == pytest.approx(1.0)
    assert sim.position("BTCUSDT") == 0.0
    assert sim.summary()["symbols"]["BTCUSDT"]["cash"] == pytest.approx(1_002.0)

    # No inventory left; another sell attempt must not short.
    sim.apply_signal(_signal(bid=None, ask=101.0, ask_qty=1.0))
    assert sim.on_trade(_trade(101.0, 1.0, trade_id=3)) == []
    assert sim.position("BTCUSDT") == 0.0


def test_long_only_cash_limits_buy_quantity() -> None:
    sim = Simulator(_cfg(initial_cash_usd=50.0))
    sim.apply_signal(_signal(bid=100.0, bid_qty=1.0, ask=None, ask_qty=0.0))
    fills = sim.on_trade(_trade(100.0, 1.0))
    assert len(fills) == 1
    assert fills[0].qty == pytest.approx(0.5)
    assert sim.position("BTCUSDT") == pytest.approx(0.5)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["cash"] == pytest.approx(0.0)
    assert summary["initial_cash"] == pytest.approx(50.0)


def test_long_only_pnl_is_relative_to_initial_cash() -> None:
    sim = Simulator(_cfg())
    sim.apply_signal(_signal())
    sim.on_trade(_trade(99.0, 1.0, trade_id=1))
    sim.apply_signal(_signal(bid=None, ask=101.0))
    sim.on_trade(_trade(101.0, 1.0, trade_id=2))
    sim.mark("BTCUSDT", 100.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["total_pnl"] == pytest.approx(2.0)
    assert summary["total_pnl"] == pytest.approx(
        summary["realized_pnl"] + summary["unrealized_pnl"]
    )


def test_strategy_suppresses_ask_without_inventory_in_long_only() -> None:
    mm = MarketMaker(_cfg())
    sig = mm.on_snapshot(_snapshot(1_000), position=0.0)
    assert sig is not None
    assert sig.bid_price is not None
    assert sig.ask_price is None
    assert sig.ask_qty == 0.0

    sig = mm.on_snapshot(_snapshot(2_000), position=0.01)
    assert sig is not None
    assert sig.ask_price is not None
    assert 0.0 < sig.ask_qty <= 0.01

    sig = mm.on_snapshot(_snapshot(3_000), position=MIN_QTY / 2)
    assert sig is not None
    assert sig.ask_price is None


def test_long_only_config_requires_positive_initial_cash() -> None:
    cfg = Config()
    cfg.strategy.allow_short = False
    cfg.strategy.initial_cash_usd = 0.0
    with pytest.raises(ValueError):
        cfg.validate()
