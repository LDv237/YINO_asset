"""Unit and property tests for the deterministic execution simulator."""

from __future__ import annotations

import math
import random

import pytest

from tickforge.config import StrategyConfig
from tickforge.events import TICKS_PER_UNIT, Fill, Signal, TradeEvent
from tickforge.exec.simulator import Simulator


def to_ticks(value: float) -> int:
    return int(round(value * TICKS_PER_UNIT))


def make_cfg(**overrides: object) -> StrategyConfig:
    return StrategyConfig(**overrides)  # type: ignore[arg-type]


def make_signal(
    symbol: str = "BTCUSDT",
    *,
    bid: float | None = 99.0,
    bid_qty: float = 1.0,
    ask: float | None = 101.0,
    ask_qty: float = 1.0,
    ts_ms: int = 1_000,
    seq: int = 1,
) -> Signal:
    fair = 100.0
    if bid is not None and ask is not None:
        fair = (bid + ask) / 2.0
    return Signal(
        symbol=symbol,
        ts_ms=ts_ms,
        seq=seq,
        fair_price=fair,
        bid_price=bid,
        bid_qty=bid_qty,
        ask_price=ask,
        ask_qty=ask_qty,
        reason="mm",
    )


def make_trade(
    symbol: str = "BTCUSDT",
    *,
    price: float = 100.0,
    qty: float = 1.0,
    trade_id: int = 1,
    ts_ms: int = 1_000,
    trade_time_ms: int | None = None,
    seq: int = 1,
) -> TradeEvent:
    return TradeEvent(
        symbol=symbol,
        event_time_ms=ts_ms,
        trade_time_ms=trade_time_ms,
        agg_trade_id=trade_id,
        price_ticks=to_ticks(price),
        qty_ticks=to_ticks(qty),
        is_buyer_maker=False,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=seq,
    )


def make_sim(**cfg_overrides: object) -> Simulator:
    return Simulator(make_cfg(**cfg_overrides))


def assert_fills_finite(fills: list[Fill]) -> None:
    for fill in fills:
        for name in ("price", "qty", "fee", "position", "cash", "realized_pnl"):
            value = getattr(fill, name)
            assert math.isfinite(value), f"{name} is not finite: {value!r}"
            assert fill.to_dict()[name] == value


# ---------------------------------------------------------------------------
# Basic fills
# ---------------------------------------------------------------------------
def test_trade_before_quote_timestamp_does_not_fill() -> None:
    sim = make_sim()
    sim.apply_signal(
        make_signal(bid=100.0, bid_qty=1.0, ask=101.0, ask_qty=1.0, ts_ms=200_000)
    )

    assert sim.on_trade(make_trade(price=99.0, ts_ms=199_999, seq=1)) == []
    assert sim.on_trade(make_trade(price=102.0, ts_ms=199_999, seq=2)) == []

    buy = sim.on_trade(make_trade(price=99.0, ts_ms=200_000, trade_id=3, seq=3))
    sell = sim.on_trade(make_trade(price=102.0, ts_ms=200_001, trade_id=4, seq=4))
    assert len(buy) == 1 and buy[0].side == "BUY"
    assert len(sell) == 1 and sell[0].side == "SELL"

def test_trade_execution_time_controls_fill_not_event_time() -> None:
    """E >= quote_ts > T must not fill; T is the actual trade time."""

    sim = make_sim()
    sim.apply_signal(
        make_signal(bid=100.0, bid_qty=1.0, ask=101.0, ask_qty=1.0, ts_ms=150_000)
    )

    # Trade happened before the quote, but the exchange published it after.
    assert sim.on_trade(
        make_trade(price=99.0, ts_ms=150_002, trade_time_ms=149_999, seq=1)
    ) == []
    assert sim.on_trade(
        make_trade(price=102.0, ts_ms=150_002, trade_time_ms=149_999, trade_id=2, seq=2)
    ) == []

    # Same millisecond as the quote is allowed; the fill carries T, not E.
    fills = sim.on_trade(
        make_trade(
            price=99.0,
            ts_ms=150_010,
            trade_time_ms=150_000,
            trade_id=3,
            seq=3,
        )
    )
    assert len(fills) == 1 and fills[0].side == "BUY"
    assert fills[0].ts_ms == 150_000


def test_buy_fill_at_quote_price_with_fee() -> None:
    sim = make_sim(fee_bps=1.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=2.0, ask=102.0, ask_qty=2.0))

    fills = sim.on_trade(make_trade(price=99.0, qty=0.5, trade_id=77, ts_ms=1_234))

    assert len(fills) == 1
    fill = fills[0]
    assert fill.side == "BUY"
    assert fill.price == 100.0
    assert fill.qty == 0.5
    assert fill.fee == pytest.approx(100.0 * 0.5 * 1e-4)
    assert fill.position == pytest.approx(0.5)
    assert fill.cash == pytest.approx(-(100.0 * 0.5 + fill.fee))
    assert fill.realized_pnl == pytest.approx(-fill.fee)
    assert fill.trade_id == 77
    assert fill.ts_ms == 1_234
    assert fill.reason == "maker"
    assert sim.position("BTCUSDT") == pytest.approx(0.5)
    assert_fills_finite(fills)

    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["avg_cost"] == pytest.approx(100.0)
    assert summary["open_bid"] == 100.0
    assert summary["open_bid_qty"] == pytest.approx(1.5)
    assert summary["open_ask"] == 102.0


def test_sell_fill_at_quote_price_with_fee() -> None:
    sim = make_sim(fee_bps=1.0)
    sim.apply_signal(make_signal(bid=98.0, ask=100.0, ask_qty=2.0))

    fills = sim.on_trade(make_trade(price=101.0, qty=0.25, trade_id=5))

    assert len(fills) == 1
    fill = fills[0]
    assert fill.side == "SELL"
    assert fill.price == 100.0
    assert fill.qty == 0.25
    assert fill.fee == pytest.approx(100.0 * 0.25 * 1e-4)
    assert fill.position == pytest.approx(-0.25)
    assert fill.realized_pnl == pytest.approx(-fill.fee)
    assert fill.cash == pytest.approx(100.0 * 0.25 - fill.fee)


def test_price_crossing_conditions_are_inclusive() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, ask=100.5, bid_qty=1.0, ask_qty=1.0))

    # Trade exactly at bid triggers the buy; one tick above does not.
    assert len(sim.on_trade(make_trade(price=100.0, qty=0.1))) == 1
    # Bid quote still has 0.9 left. One tick above the bid: no buy.
    assert sim.on_trade(make_trade(price=100.00000001, qty=0.1)) == []

    sim2 = make_sim(fee_bps=0.0)
    sim2.apply_signal(make_signal(bid=99.0, ask=100.0, ask_qty=1.0))
    # Trade exactly at ask triggers the sell.
    assert len(sim2.on_trade(make_trade(price=100.0, qty=0.1))) == 1
    assert sim2.on_trade(make_trade(price=99.99999999, qty=0.1)) == []


def test_no_fill_when_trade_does_not_cross() -> None:
    sim = make_sim()
    sim.apply_signal(make_signal(bid=99.0, ask=101.0))
    assert sim.on_trade(make_trade(price=100.0, qty=1.0)) == []
    assert sim.position("BTCUSDT") == 0.0


def test_zero_quantity_trade_is_ignored() -> None:
    sim = make_sim()
    sim.apply_signal(make_signal(bid=99.0, ask=101.0))
    trade = TradeEvent(
        symbol="BTCUSDT",
        event_time_ms=1_000,
        agg_trade_id=1,
        price_ticks=to_ticks(99.0),
        qty_ticks=0,
        is_buyer_maker=False,
        recv_mono_ns=0,
        recv_wall_ns=0,
        seq=1,
    )
    assert sim.on_trade(trade) == []


# ---------------------------------------------------------------------------
# Partial fills
# ---------------------------------------------------------------------------
def test_partial_fills_consume_the_resting_quote() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=1.0, ask=110.0, ask_qty=1.0))

    first = sim.on_trade(make_trade(price=99.0, qty=0.4, trade_id=1))
    assert [f.qty for f in first] == [0.4]
    assert sim.summary()["symbols"]["BTCUSDT"]["open_bid_qty"] == pytest.approx(0.6)

    second = sim.on_trade(make_trade(price=99.0, qty=0.7, trade_id=2))
    assert [f.qty for f in second] == [pytest.approx(0.6)]

    third = sim.on_trade(make_trade(price=99.0, qty=0.7, trade_id=3))
    assert third == []
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["open_bid"] is None
    assert summary["open_bid_qty"] == 0.0
    assert summary["position"] == pytest.approx(1.0)
    assert summary["fills"] == 2


def test_both_sides_can_fill_from_one_trade_buy_first() -> None:
    sim = make_sim(fee_bps=0.0)
    # Deliberately crossed quotes: the trade can hit both sides.
    sim.apply_signal(make_signal(bid=100.5, ask=99.5, bid_qty=1.0, ask_qty=1.0))

    fills = sim.on_trade(make_trade(price=100.0, qty=1.0))

    assert [f.side for f in fills] == ["BUY", "SELL"]
    assert [f.price for f in fills] == [100.5, 99.5]
    assert [f.position for f in fills] == [pytest.approx(1.0), pytest.approx(0.0)]
    # Bought at 100.5, sold at 99.5: loss of 1.0 with zero fees.
    assert fills[-1].realized_pnl == pytest.approx(-1.0)
    assert fills[-1].cash == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Fees
# ---------------------------------------------------------------------------
def test_fee_math_across_buy_and_sell() -> None:
    sim = make_sim(fee_bps=2.5)  # 0.025 %
    sim.apply_signal(make_signal(bid=200.0, bid_qty=1.0, ask=220.0, ask_qty=1.0))

    buy = sim.on_trade(make_trade(price=200.0, qty=0.3))[0]
    assert buy.fee == pytest.approx(200.0 * 0.3 * 2.5 / 1e4)
    assert buy.fee == pytest.approx(0.015)

    sell = sim.on_trade(make_trade(price=220.0, qty=0.1))[0]
    assert sell.fee == pytest.approx(220.0 * 0.1 * 2.5 / 1e4)
    assert sell.realized_pnl == pytest.approx(
        -0.015 + (220.0 - 200.0) * 0.1 - 0.0055
    )
    assert sell.cash == pytest.approx(-(200.0 * 0.3 + 0.015) + 220.0 * 0.1 - 0.0055)


def test_realized_pnl_is_cumulative_and_fee_inclusive() -> None:
    sim = make_sim(fee_bps=10.0)  # 0.1 %
    sim.apply_signal(make_signal(bid=100.0, ask=110.0))

    buy_fill = sim.on_trade(make_trade(price=100.0, qty=1.0))[0]
    assert buy_fill.realized_pnl == pytest.approx(-0.1)

    sell_fill = sim.on_trade(make_trade(price=110.0, qty=1.0))[0]
    # -0.1 (buy fee) + 10.0 gross - 0.11 (sell fee)
    assert sell_fill.realized_pnl == pytest.approx(9.79)
    assert sell_fill.cash == pytest.approx(9.79)
    assert sim.position("BTCUSDT") == 0.0
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["fees"] == pytest.approx(0.21)
    assert summary["realized_pnl"] == pytest.approx(9.79)
    assert summary["total_pnl"] == pytest.approx(9.79)


# ---------------------------------------------------------------------------
# Position limits
# ---------------------------------------------------------------------------
def test_max_position_truncates_buy_then_rejects() -> None:
    sim = make_sim(max_position_notional_usd=1_000.0, fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=20.0, ask=None, ask_qty=0.0))

    fills = sim.on_trade(make_trade(price=100.0, qty=20.0))
    assert len(fills) == 1
    assert fills[0].qty == pytest.approx(10.0)  # 1000 USD / 100
    assert fills[0].position == pytest.approx(10.0)
    assert abs(fills[0].position) * fills[0].price <= 1_000.0 + 1e-9

    assert sim.on_trade(make_trade(price=100.0, qty=1.0)) == []
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["risk_truncated"] == 1
    assert summary["risk_rejected"] == 1
    assert summary["max_abs_position"] == pytest.approx(10.0)


def test_max_position_limits_short_side() -> None:
    sim = make_sim(max_position_notional_usd=500.0, fee_bps=0.0)
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=100.0, ask_qty=20.0))

    fills = sim.on_trade(make_trade(price=100.0, qty=20.0))
    assert len(fills) == 1
    assert fills[0].qty == pytest.approx(5.0)
    assert fills[0].position == pytest.approx(-5.0)

    assert sim.on_trade(make_trade(price=100.0, qty=1.0)) == []
    assert sim.summary()["symbols"]["BTCUSDT"]["risk_rejected"] == 1


def test_risk_cap_uses_fill_price_and_allows_reducing_exposure() -> None:
    sim = make_sim(max_position_notional_usd=1_000.0, fee_bps=0.0)
    # Build a long position of 10 units at 100.
    sim.apply_signal(make_signal(bid=100.0, bid_qty=10.0, ask=None, ask_qty=0.0))
    assert sim.on_trade(make_trade(price=100.0, qty=10.0))[0].qty == pytest.approx(10.0)

    # The position is now worth 2000 at a price of 200: buying is rejected...
    sim.apply_signal(make_signal(bid=200.0, bid_qty=1.0, ask=None, ask_qty=0.0))
    assert sim.on_trade(make_trade(price=200.0, qty=1.0)) == []
    # ...but selling (reducing exposure) is still allowed.
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=200.0, ask_qty=10.0))
    fills = sim.on_trade(make_trade(price=200.0, qty=10.0))
    assert len(fills) == 1
    assert fills[0].qty == pytest.approx(10.0)
    assert sim.position("BTCUSDT") == 0.0


# ---------------------------------------------------------------------------
# Average cost accounting
# ---------------------------------------------------------------------------
def test_average_cost_and_realized_pnl() -> None:
    sim = make_sim(fee_bps=0.0)

    sim.apply_signal(make_signal(bid=100.0, bid_qty=5.0, ask=None, ask_qty=0.0))
    sim.on_trade(make_trade(price=100.0, qty=2.0, trade_id=1))
    sim.apply_signal(make_signal(bid=102.0, bid_qty=5.0, ask=None, ask_qty=0.0))
    sim.on_trade(make_trade(price=102.0, qty=1.0, trade_id=2))

    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == pytest.approx(3.0)
    assert summary["avg_cost"] == pytest.approx((100.0 * 2 + 102.0) / 3.0)
    assert summary["cash"] == pytest.approx(-302.0)

    avg = (100.0 * 2 + 102.0) / 3.0
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=110.0, ask_qty=5.0))
    sell = sim.on_trade(make_trade(price=110.0, qty=0.5, trade_id=3))[0]
    assert sell.realized_pnl == pytest.approx((110.0 - avg) * 0.5)

    sim.mark("BTCUSDT", 110.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == pytest.approx(2.5)
    assert summary["avg_cost"] == pytest.approx(avg)
    assert summary["unrealized_pnl"] == pytest.approx((110.0 - avg) * 2.5)
    assert summary["total_pnl"] == pytest.approx(summary["cash"] + 2.5 * 110.0)
    assert summary["total_pnl"] == pytest.approx(
        summary["realized_pnl"] + summary["unrealized_pnl"]
    )

    # Close the rest; average cost resets to zero.
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=90.0, ask_qty=5.0))
    sim.on_trade(make_trade(price=90.0, qty=2.5, trade_id=4))
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == 0.0
    assert summary["avg_cost"] == 0.0
    assert summary["unrealized_pnl"] == 0.0
    assert summary["total_pnl"] == pytest.approx(summary["realized_pnl"])
    assert summary["total_pnl"] == pytest.approx(summary["cash"])


def test_short_average_cost_and_covering() -> None:
    sim = make_sim(fee_bps=0.0)

    # Open a short at 100.
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=100.0, ask_qty=5.0))
    sim.on_trade(make_trade(price=100.0, qty=1.0, trade_id=1))
    # Add another unit at 90 -> average entry 95.
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=90.0, ask_qty=5.0))
    sim.on_trade(make_trade(price=90.0, qty=1.0, trade_id=2))

    sim.mark("BTCUSDT", 90.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == pytest.approx(-2.0)
    assert summary["avg_cost"] == pytest.approx(95.0)
    assert summary["unrealized_pnl"] == pytest.approx(10.0)
    assert summary["total_pnl"] == pytest.approx(
        summary["cash"] + summary["position"] * 90.0
    )

    # Buy back one unit at 80: realised (95-80) = 15.
    sim.apply_signal(make_signal(bid=80.0, bid_qty=5.0, ask=None, ask_qty=0.0))
    buy = sim.on_trade(make_trade(price=80.0, qty=1.0, trade_id=3))[0]
    assert buy.realized_pnl == pytest.approx(15.0)

    sim.mark("BTCUSDT", 90.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == pytest.approx(-1.0)
    assert summary["avg_cost"] == pytest.approx(95.0)
    assert summary["realized_pnl"] == pytest.approx(15.0)
    assert summary["unrealized_pnl"] == pytest.approx(5.0)
    assert summary["total_pnl"] == pytest.approx(20.0)


def test_position_can_cross_from_long_to_short() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=2.0, ask=None, ask_qty=0.0))
    sim.on_trade(make_trade(price=100.0, qty=2.0))

    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=120.0, ask_qty=5.0))
    fill = sim.on_trade(make_trade(price=120.0, qty=3.0))[0]

    assert fill.position == pytest.approx(-1.0)
    assert fill.realized_pnl == pytest.approx((120.0 - 100.0) * 2.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["avg_cost"] == pytest.approx(120.0)
    assert summary["cash"] == pytest.approx(-200.0 + 360.0)


# ---------------------------------------------------------------------------
# Cancel, marks, summary
# ---------------------------------------------------------------------------
def test_cancel_removes_quotes_but_keeps_position() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=1.0, ask=101.0, ask_qty=1.0))
    sim.on_trade(make_trade(price=100.0, qty=1.0))
    assert sim.position("BTCUSDT") == pytest.approx(1.0)

    sim.cancel("BTCUSDT")
    assert sim.on_trade(make_trade(price=99.0, qty=1.0)) == []
    assert sim.on_trade(make_trade(price=102.0, qty=1.0)) == []

    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["position"] == pytest.approx(1.0)
    assert summary["open_bid"] is None
    assert summary["open_ask"] is None
    assert summary["open_bid_qty"] == 0.0
    assert summary["open_ask_qty"] == 0.0


def test_apply_signal_replaces_previous_quotes() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, bid_qty=1.0, ask=102.0, ask_qty=1.0))
    sim.apply_signal(make_signal(bid=90.0, bid_qty=2.0, ask=None, ask_qty=0.0))

    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["open_bid"] == 90.0
    assert summary["open_bid_qty"] == 2.0
    assert summary["open_ask"] is None

    # A price that would have crossed the old ask now does nothing.
    assert sim.on_trade(make_trade(price=101.0, qty=1.0)) == []
    assert len(sim.on_trade(make_trade(price=90.0, qty=1.0))) == 1


def test_mark_ignores_invalid_values() -> None:
    sim = make_sim(fee_bps=0.0)
    sim.apply_signal(make_signal(bid=100.0, ask=101.0))
    sim.on_trade(make_trade(price=100.0, qty=1.0))
    sim.mark("BTCUSDT", 105.0)
    sim.mark("BTCUSDT", float("nan"))
    sim.mark("BTCUSDT", 0.0)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["mark"] == 105.0
    assert summary["unrealized_pnl"] == pytest.approx(5.0)


def test_position_and_summary_for_unknown_symbol() -> None:
    sim = make_sim()
    assert sim.position("NOPE") == 0.0
    assert "NOPE" not in sim.summary()["symbols"]


def test_precreated_symbols_appear_in_summary() -> None:
    sim = Simulator(make_cfg(), symbols=["BTCUSDT", "ETHUSDT", "BTCUSDT"])
    summary = sim.summary()
    assert list(summary["symbols"]) == ["BTCUSDT", "ETHUSDT"]
    for row in summary["symbols"].values():
        assert row["position"] == 0.0
        assert row["fills"] == 0
        assert row["open_bid"] is None
    assert summary["totals"]["fills"] == 0


def test_summary_reconciles_across_symbols() -> None:
    sim = make_sim(fee_bps=1.0, max_position_notional_usd=10_000.0)

    sim.apply_signal(make_signal("BTCUSDT", bid=100.0, bid_qty=2.0, ask=101.0, ask_qty=2.0))
    sim.apply_signal(make_signal("ETHUSDT", bid=50.0, bid_qty=4.0, ask=51.0, ask_qty=4.0))

    all_fills: list[Fill] = []
    all_fills += sim.on_trade(make_trade("BTCUSDT", price=100.0, qty=1.0, trade_id=1))
    all_fills += sim.on_trade(make_trade("ETHUSDT", price=50.0, qty=3.0, trade_id=2))
    all_fills += sim.on_trade(make_trade("BTCUSDT", price=101.0, qty=0.5, trade_id=3))
    all_fills += sim.on_trade(make_trade("ETHUSDT", price=51.0, qty=1.0, trade_id=4))

    sim.mark("BTCUSDT", 102.0)
    sim.mark("ETHUSDT", 49.0)

    summary = sim.summary()
    symbols = summary["symbols"]
    totals = summary["totals"]

    assert set(symbols) == {"BTCUSDT", "ETHUSDT"}
    assert totals["fills"] == len(all_fills) == 4
    assert totals["position"] == pytest.approx(
        symbols["BTCUSDT"]["position"] + symbols["ETHUSDT"]["position"]
    )
    assert totals["cash"] == pytest.approx(
        symbols["BTCUSDT"]["cash"] + symbols["ETHUSDT"]["cash"]
    )
    assert totals["realized_pnl"] == pytest.approx(
        symbols["BTCUSDT"]["realized_pnl"] + symbols["ETHUSDT"]["realized_pnl"]
    )
    assert totals["unrealized_pnl"] == pytest.approx(
        symbols["BTCUSDT"]["unrealized_pnl"] + symbols["ETHUSDT"]["unrealized_pnl"]
    )
    assert totals["total_pnl"] == pytest.approx(
        symbols["BTCUSDT"]["total_pnl"] + symbols["ETHUSDT"]["total_pnl"]
    )
    assert totals["fees"] == pytest.approx(
        symbols["BTCUSDT"]["fees"] + symbols["ETHUSDT"]["fees"]
    )

    for row in symbols.values():
        assert row["total_pnl"] == pytest.approx(
            row["cash"] + row["position"] * row["mark"]
        )
        assert row["total_pnl"] == pytest.approx(
            row["realized_pnl"] + row["unrealized_pnl"]
        )
        assert row["fees"] >= 0.0
        assert row["max_abs_position"] >= abs(row["position"])
        assert_fills_finite(all_fills)


# ---------------------------------------------------------------------------
# Determinism and property-style checks
# ---------------------------------------------------------------------------
def _run_scripted_scenario() -> tuple[list[Fill], dict[str, object]]:
    sim = Simulator(
        make_cfg(fee_bps=1.5, max_position_notional_usd=2_000.0),
        symbols=["BTCUSDT", "ETHUSDT"],
    )
    emitted: list[Fill] = []

    script = [
        ("signal", make_signal("BTCUSDT", bid=100.0, bid_qty=7.0, ask=101.0, ask_qty=7.0)),
        ("trade", make_trade("BTCUSDT", price=100.0, qty=2.5, trade_id=1)),
        ("trade", make_trade("BTCUSDT", price=101.0, qty=1.5, trade_id=2)),
        ("signal", make_signal("ETHUSDT", bid=40.0, bid_qty=30.0, ask=41.0, ask_qty=30.0)),
        ("trade", make_trade("ETHUSDT", price=40.5, qty=10.0, trade_id=3)),
        ("mark", ("BTCUSDT", 100.75)),
        ("mark", ("ETHUSDT", 40.25)),
        ("signal", make_signal("BTCUSDT", bid=99.5, bid_qty=7.0, ask=100.5, ask_qty=7.0)),
        ("trade", make_trade("BTCUSDT", price=99.5, qty=100.0, trade_id=4)),
        ("trade", make_trade("BTCUSDT", price=100.5, qty=100.0, trade_id=5)),
        ("cancel", "ETHUSDT"),
        ("trade", make_trade("ETHUSDT", price=35.0, qty=1.0, trade_id=6)),
        ("trade", make_trade("ETHUSDT", price=45.0, qty=1.0, trade_id=7)),
    ]
    for kind, payload in script:
        if kind == "signal":
            sim.apply_signal(payload)  # type: ignore[arg-type]
        elif kind == "trade":
            emitted.extend(sim.on_trade(payload))  # type: ignore[arg-type]
        elif kind == "mark":
            symbol, mid = payload  # type: ignore[misc]
            sim.mark(symbol, mid)
        else:
            sim.cancel(payload)  # type: ignore[arg-type]
    return emitted, sim.summary()


def test_two_identical_simulations_produce_identical_fills() -> None:
    fills_a, summary_a = _run_scripted_scenario()
    fills_b, summary_b = _run_scripted_scenario()

    assert [f.to_dict() for f in fills_a] == [f.to_dict() for f in fills_b]
    assert summary_a == summary_b
    assert fills_a  # the scenario must actually trade


def test_fill_sequence_is_deterministic_with_randomised_but_seeded_flow() -> None:
    def run_once() -> list[dict[str, object]]:
        rng = random.Random(123456)
        sim = make_sim(fee_bps=2.0, max_position_notional_usd=1_500.0)
        out: list[Fill] = []
        price = 100.0
        for i in range(300):
            price = max(1.0, price * (1.0 + rng.uniform(-0.01, 0.01)))
            spread = rng.uniform(0.1, 1.0)
            sim.apply_signal(
                make_signal(
                    bid=price - spread,
                    bid_qty=rng.uniform(0.1, 5.0),
                    ask=price + spread,
                    ask_qty=rng.uniform(0.1, 5.0),
                    ts_ms=1_000 + i,
                    seq=i,
                )
            )
            trade_price = max(0.5, price * (1.0 + rng.uniform(-0.002, 0.002)))
            out.extend(
                sim.on_trade(
                    make_trade(
                        price=trade_price,
                        qty=rng.uniform(0.01, 4.0),
                        trade_id=i,
                        ts_ms=1_000 + i,
                        seq=i,
                    )
                )
            )
            if i % 7 == 0:
                sim.mark("BTCUSDT", price)
        return [f.to_dict() for f in out]

    assert run_once() == run_once()


def test_property_fills_respect_risk_limit_and_accounting_identity() -> None:
    rng = random.Random(99)
    max_notional = 2_000.0
    fee_bps = 3.0
    sim = Simulator(make_cfg(max_position_notional_usd=max_notional, fee_bps=fee_bps))
    price = 100.0
    fills: list[Fill] = []

    for i in range(500):
        price = max(1.0, price * (1.0 + rng.uniform(-0.02, 0.02)))
        spread = rng.uniform(0.05, 0.5)
        sim.apply_signal(
            make_signal(
                bid=price - spread,
                bid_qty=rng.uniform(0.05, 3.0),
                ask=price + spread,
                ask_qty=rng.uniform(0.05, 3.0),
                ts_ms=i,
                seq=i,
            )
        )
        # Trades sometimes cross the bid, sometimes the ask, sometimes both.
        for j in range(rng.randint(1, 2)):
            trade_price = price + rng.uniform(-1.5 * spread, 1.5 * spread)
            new_fills = sim.on_trade(
                make_trade(
                    price=max(0.01, trade_price),
                    qty=rng.uniform(0.001, 2.0),
                    trade_id=i * 2 + j,
                    ts_ms=i,
                    seq=i,
                )
            )
            fills.extend(new_fills)
            for fill in new_fills:
                assert math.isfinite(fill.price) and fill.price > 0.0
                assert math.isfinite(fill.qty) and fill.qty > 0.0
                assert fill.fee >= 0.0 and math.isfinite(fill.fee)
                signed_qty = fill.qty if fill.side == "BUY" else -fill.qty
                prev_position = fill.position - signed_qty
                prev_notional = abs(prev_position) * fill.price
                post_notional = abs(fill.position) * fill.price
                if prev_notional <= max_notional * (1.0 + 1e-12) + 1e-9:
                    # Exposure-increasing fills never cross the cap; the cap
                    # is evaluated at the fill price, not a moving mark.
                    assert post_notional <= max_notional * (1.0 + 1e-12) + 1e-9
                else:
                    # If price moves after a position is opened, only
                    # risk-reducing fills are possible in this state.
                    assert abs(fill.position) <= abs(prev_position) + 1e-12
                assert fill.side in ("BUY", "SELL")
        if i % 11 == 0:
            sim.mark("BTCUSDT", price)

    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["fills"] == len(fills)
    assert summary["fees"] == pytest.approx(sum(abs(f.fee) for f in fills))
    assert summary["total_pnl"] == pytest.approx(
        summary["realized_pnl"] + summary["unrealized_pnl"], abs=1e-6
    )
    assert summary["total_pnl"] == pytest.approx(
        summary["cash"] + summary["position"] * summary["mark"], abs=1e-6
    )


def test_over_limit_position_only_allows_reducing_fills() -> None:
    sim = Simulator(
        make_cfg(max_position_notional_usd=1_000.0, fee_bps=0.0)
    )
    sim.apply_signal(
        make_signal(bid=100.0, bid_qty=10.0, ask=None, ask_qty=0.0)
    )
    first = sim.on_trade(make_trade(price=100.0, qty=10.0))
    assert len(first) == 1
    assert first[0].position == pytest.approx(10.0)

    # Mark price doubles; the position is now over the cap at the current
    # price.  A sell that reduces exposure must still be allowed.
    sim.apply_signal(
        make_signal(bid=None, bid_qty=0.0, ask=200.0, ask_qty=0.1)
    )
    second = sim.on_trade(make_trade(price=200.0, qty=0.1, trade_id=2))
    assert len(second) == 1
    assert second[0].position == pytest.approx(9.9)
    summary = sim.summary()["symbols"]["BTCUSDT"]
    assert summary["risk_rejected"] == 0


def test_nonfinite_scaled_quote_is_dropped_not_raised() -> None:
    sim = Simulator(make_cfg())
    # Finite float, but overflows after multiplying by 1e8.
    sim.apply_signal(make_signal(bid=1e305, bid_qty=1.0, ask=None, ask_qty=0.0))
    sim.apply_signal(make_signal(bid=None, bid_qty=0.0, ask=1e305, ask_qty=1.0))
    assert sim.summary()["symbols"]["BTCUSDT"]["position"] == 0.0


def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError):
        Simulator(make_cfg(fee_bps=-1.0))
    with pytest.raises(ValueError):
        Simulator(make_cfg(max_position_notional_usd=0.0))
    with pytest.raises(ValueError):
        Simulator(make_cfg(max_position_notional_usd=float("nan")))
