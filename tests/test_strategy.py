"""Unit tests for the deterministic demo market maker."""

from __future__ import annotations

import math

import pytest

from tickforge.config import StrategyConfig
from tickforge.events import FactorSnapshot
from tickforge.strategy.mm import MIN_QTY, MarketMaker


def make_snap(**overrides: object) -> FactorSnapshot:
    values: dict[str, object] = {
        "symbol": "BTCUSDT",
        "ts_ms": 1_000,
        "seq": 1,
        "mid": 100.0,
        "microprice": 100.0,
        "spread_bps": 1.0,
        "ofi_1s": 0.0,
        "depth_imbalance_5": 0.0,
        "trade_imbalance_1s": 0.0,
        "realized_vol_bps": 0.0,
        "momentum_5s": 0.0,
        "best_bid": 99.995,
        "best_ask": 100.005,
        "bid_qty": 10.0,
        "ask_qty": 10.0,
    }
    values.update(overrides)
    return FactorSnapshot(**values)  # type: ignore[arg-type]


def make_cfg(**overrides: object) -> StrategyConfig:
    return StrategyConfig(**overrides)  # type: ignore[arg-type]


def make_mm(**overrides: object) -> MarketMaker:
    return MarketMaker(make_cfg(**overrides))


# ---------------------------------------------------------------------------
# Fair value and quoting formulas
# ---------------------------------------------------------------------------
def test_fair_price_formula_matches_interface() -> None:
    mm = make_mm()
    snap = make_snap(microprice=100.0, momentum_5s=0.001, ofi_1s=5.0,
                     bid_qty=10.0, ask_qty=10.0)

    sig = mm.on_snapshot(snap, position=0.0)

    assert sig is not None
    # ofi_norm = 5 / 20 = 0.25; 100 * (1 + 0.35*0.001 + 0.8e-4*0.25)
    expected_fair = 100.0 * (1.0 + 0.35 * 0.001 + 0.8e-4 * 0.25)
    assert sig.fair_price == pytest.approx(expected_fair, rel=1e-12)
    assert sig.symbol == "BTCUSDT"
    assert sig.seq == snap.seq
    assert sig.ts_ms == snap.ts_ms
    assert sig.reason == "mm"


def test_ofi_normalisation_is_clamped() -> None:
    mm = make_mm()
    # ofi_1s / (bid_qty+ask_qty) = 1e6 / 2 -> would be 500k, clamped to +3.
    sig = mm.on_snapshot(make_snap(ofi_1s=1e6, bid_qty=1.0, ask_qty=1.0), position=0.0)
    assert sig is not None
    assert sig.fair_price == pytest.approx(100.0 * (1.0 + 0.8e-4 * 3.0), rel=1e-12)

    mm2 = make_mm()
    sig2 = mm2.on_snapshot(make_snap(ofi_1s=-1e6, bid_qty=1.0, ask_qty=1.0), position=0.0)
    assert sig2 is not None
    assert sig2.fair_price == pytest.approx(100.0 * (1.0 - 0.8e-4 * 3.0), rel=1e-12)


def test_round_to_tick_rounds_bid_down_and_ask_up() -> None:
    mm = make_mm(round_to_tick=True)
    snap = make_snap(
        symbol="ETHUSDT",
        mid=2393.6395,
        microprice=2393.6395,
        best_bid=2393.63,
        best_ask=2393.65,
    )

    sig = mm.on_snapshot(snap, position=0.0)

    assert sig is not None
    assert sig.bid_price is not None and sig.ask_price is not None
    assert abs(sig.bid_price * 100 - round(sig.bid_price * 100)) < 1e-9
    assert abs(sig.ask_price * 100 - round(sig.ask_price * 100)) < 1e-9
    assert sig.bid_price <= 2393.6395 <= sig.ask_price


def test_half_spread_formula_and_clamp() -> None:
    # vol = 10 bps -> half = 0.8 + 0.6*10 = 6.8 bps
    mm = make_mm()
    sig = mm.on_snapshot(make_snap(realized_vol_bps=10.0), position=0.0)
    assert sig is not None
    fair = 100.0
    assert sig.bid_price == pytest.approx(fair * (1 - 6.8 / 1e4), rel=1e-12)
    assert sig.ask_price == pytest.approx(fair * (1 + 6.8 / 1e4), rel=1e-12)

    # Huge volatility clamps at max_half_spread_bps.
    mm_hi = make_mm()
    sig_hi = mm_hi.on_snapshot(make_snap(realized_vol_bps=10_000.0), position=0.0)
    assert sig_hi is not None
    assert sig_hi.bid_price == pytest.approx(100.0 * (1 - 25.0 / 1e4), rel=1e-12)
    assert sig_hi.ask_price == pytest.approx(100.0 * (1 + 25.0 / 1e4), rel=1e-12)

    # Zero volatility uses the minimum half spread.
    mm_lo = make_mm()
    sig_lo = mm_lo.on_snapshot(make_snap(realized_vol_bps=0.0), position=0.0)
    assert sig_lo is not None
    assert sig_lo.bid_price == pytest.approx(100.0 * (1 - 0.8 / 1e4), rel=1e-12)


def test_inventory_skew_direction() -> None:
    mm = make_mm()  # target base = 5000 / 100 = 50
    flat = mm.on_snapshot(make_snap(ts_ms=1_000), position=0.0)
    # advance cooldown
    long_sig = mm.on_snapshot(make_snap(ts_ms=1_100), position=25.0)
    short_sig = mm.on_snapshot(make_snap(ts_ms=1_200), position=-25.0)

    assert flat is not None and long_sig is not None and short_sig is not None
    # reservation = 100 * (1 - 0.25 * 25/50) = 87.5
    long_res = 87.5
    # reservation = 100 * (1 + 0.25 * 25/50) = 112.5
    short_res = 112.5
    assert long_sig.bid_price == pytest.approx(long_res * (1 - 0.8 / 1e4), rel=1e-12)
    assert short_sig.ask_price == pytest.approx(short_res * (1 + 0.8 / 1e4), rel=1e-12)
    # Long inventory lowers both quotes, short inventory raises them.
    assert long_sig.bid_price < flat.bid_price < short_sig.bid_price
    assert long_sig.ask_price < flat.ask_price < short_sig.ask_price


def test_reservation_is_clamped_to_half_and_one_and_a_half_fair() -> None:
    mm = make_mm(inventory_skew=5.0)
    # 1 - 5*(25/50) = -1.5 -> clamp to 0.5*fair.
    low = mm.on_snapshot(make_snap(ts_ms=1_000), position=25.0)
    assert low is not None
    assert low.bid_price == pytest.approx(50.0 * (1 - 0.8 / 1e4), rel=1e-12)
    assert low.ask_price == pytest.approx(50.0 * (1 + 0.8 / 1e4), rel=1e-12)

    # 1 + 5*(25/50) = 3.5 -> clamp to 1.5*fair.
    high = mm.on_snapshot(make_snap(ts_ms=1_100), position=-25.0)
    assert high is not None
    assert high.bid_price == pytest.approx(150.0 * (1 - 0.8 / 1e4), rel=1e-12)
    assert high.ask_price == pytest.approx(150.0 * (1 + 0.8 / 1e4), rel=1e-12)


# ---------------------------------------------------------------------------
# Quantity handling
# ---------------------------------------------------------------------------
def test_quantity_is_rounded_to_eight_decimals() -> None:
    mm = make_mm(quote_notional_usd=1.0)
    sig = mm.on_snapshot(make_snap(microprice=3.0), position=0.0)
    assert sig is not None
    # 1 / 3 = 0.333333333... -> 0.33333333
    assert sig.bid_qty == 0.33333333
    assert sig.ask_qty == 0.33333333
    # The value is exactly representable at tick resolution.
    assert sig.bid_qty == round(sig.bid_qty, 8)


def test_quantity_has_minimum_of_one_tick() -> None:
    mm = make_mm(quote_notional_usd=1e-12)
    sig = mm.on_snapshot(make_snap(microprice=3.0), position=0.0)
    assert sig is not None
    assert sig.bid_qty == MIN_QTY == 1e-8
    assert sig.ask_qty == MIN_QTY


def test_default_quantity_is_quote_notional_over_fair() -> None:
    mm = make_mm()
    sig = mm.on_snapshot(make_snap(microprice=100.0), position=0.0)
    assert sig is not None
    assert sig.bid_qty == pytest.approx(500.0 / 100.0, rel=1e-12)


# ---------------------------------------------------------------------------
# Cooldown, disabled mode, invalid inputs
# ---------------------------------------------------------------------------
def test_cooldown_suppresses_repeats_per_symbol() -> None:
    mm = make_mm(quote_cooldown_ms=100)

    first = mm.on_snapshot(make_snap(ts_ms=1_000), position=0.0)
    assert first is not None

    suppressed = mm.on_snapshot(make_snap(ts_ms=1_099), position=0.0)
    assert suppressed is None

    # Exactly at the cooldown boundary a new signal is allowed.
    allowed = mm.on_snapshot(make_snap(ts_ms=1_100), position=0.0)
    assert allowed is not None

    # Another symbol has an independent cooldown.
    other = mm.on_snapshot(make_snap(symbol="ETHUSDT", ts_ms=1_050), position=0.0)
    assert other is not None

    stats = mm.stats()
    assert stats["snapshots"] == 4
    assert stats["signals"] == 3
    assert stats["cooldown_suppressed"] == 1


def test_cooldown_does_not_advance_on_suppressed_snapshot() -> None:
    mm = make_mm(quote_cooldown_ms=100)
    assert mm.on_snapshot(make_snap(ts_ms=1_000), position=0.0) is not None
    # 1_050 suppressed; the reference timestamp must remain 1_000.
    assert mm.on_snapshot(make_snap(ts_ms=1_050), position=0.0) is None
    assert mm.on_snapshot(make_snap(ts_ms=1_099), position=0.0) is None
    assert mm.on_snapshot(make_snap(ts_ms=1_100), position=0.0) is not None


def test_disabled_mode_returns_none() -> None:
    mm = make_mm(enabled=False)
    assert mm.on_snapshot(make_snap(), position=0.0) is None
    assert mm.on_snapshot(make_snap(ts_ms=2_000), position=1.0) is None
    stats = mm.stats()
    assert stats["disabled"] == 2
    assert stats["signals"] == 0
    assert stats["last_signal_ts_ms"] == {}


def test_invalid_fair_price_returns_none() -> None:
    for micro in (0.0, -1.0, float("nan"), float("inf"), float("-inf")):
        mm = make_mm()
        assert mm.on_snapshot(make_snap(microprice=micro), position=0.0) is None
        assert mm.stats()["invalid"] == 1


def test_non_finite_position_returns_none() -> None:
    mm = make_mm()
    assert mm.on_snapshot(make_snap(), position=float("nan")) is None
    assert mm.on_snapshot(make_snap(ts_ms=2_000), position=float("inf")) is None
    assert mm.stats()["invalid"] == 2


def test_non_finite_factors_are_sanitised() -> None:
    mm = make_mm()
    sig = mm.on_snapshot(
        make_snap(momentum_5s=float("nan"), ofi_1s=float("inf"),
                  bid_qty=float("nan"), ask_qty=float("inf")),
        position=0.0,
    )
    assert sig is not None
    assert sig.fair_price == pytest.approx(100.0, rel=1e-12)
    assert math.isfinite(sig.fair_price)


# ---------------------------------------------------------------------------
# Inventory limits / one-sided quoting
# ---------------------------------------------------------------------------
def test_single_side_quoting_at_inventory_limits() -> None:
    mm = make_mm()  # target_base = 50
    at_long_limit = mm.on_snapshot(make_snap(ts_ms=1_000), position=50.0)
    assert at_long_limit is not None
    assert at_long_limit.bid_price is None
    assert at_long_limit.bid_qty == 0.0
    assert at_long_limit.ask_price is not None
    assert at_long_limit.ask_qty > 0.0

    above_long_limit = mm.on_snapshot(make_snap(ts_ms=1_100), position=75.0)
    assert above_long_limit is not None
    assert above_long_limit.bid_price is None
    assert above_long_limit.ask_price is not None

    at_short_limit = mm.on_snapshot(make_snap(ts_ms=1_200), position=-50.0)
    assert at_short_limit is not None
    assert at_short_limit.ask_price is None
    assert at_short_limit.ask_qty == 0.0
    assert at_short_limit.bid_price is not None
    assert at_short_limit.bid_qty > 0.0

    below_short_limit = mm.on_snapshot(make_snap(ts_ms=1_300), position=-51.0)
    assert below_short_limit is not None
    assert below_short_limit.ask_price is None
    assert below_short_limit.bid_price is not None

    just_below_long = mm.on_snapshot(make_snap(ts_ms=1_400), position=49.999)
    assert just_below_long is not None
    assert just_below_long.bid_price is not None
    assert just_below_long.ask_price is not None


def test_stats_counts_skipped_sides() -> None:
    mm = make_mm()
    mm.on_snapshot(make_snap(ts_ms=1_000), position=50.0)  # bid skipped
    mm.on_snapshot(make_snap(ts_ms=1_100), position=-50.0)  # ask skipped
    stats = mm.stats()
    assert stats["bid_skipped"] == 1
    assert stats["ask_skipped"] == 1
    assert stats["signals"] == 2


# ---------------------------------------------------------------------------
# Determinism and output sanity
# ---------------------------------------------------------------------------
def test_signals_are_identical_for_identical_inputs() -> None:
    cfg = make_cfg()
    snaps = [
        make_snap(ts_ms=1_000 + i * 100, seq=i, microprice=100.0 + 0.01 * i,
                  momentum_5s=0.0001 * (i - 3), ofi_1s=i - 2.0,
                  realized_vol_bps=2.0 * i)
        for i in range(10)
    ]
    positions = [0.0, 5.0, -7.5, 20.0, 50.0, 49.0, -50.0, 0.0, 1.0, -1.0]

    a = MarketMaker(cfg)
    b = MarketMaker(cfg)
    seq_a = [a.on_snapshot(s, position=p) for s, p in zip(snaps, positions)]
    seq_b = [b.on_snapshot(s, position=p) for s, p in zip(snaps, positions)]

    dict_a = [s.to_dict() if s is not None else None for s in seq_a]
    dict_b = [s.to_dict() if s is not None else None for s in seq_b]
    assert dict_a == dict_b
    assert any(s is not None for s in seq_a)


def test_quote_invariants_hold_for_a_grid_of_inputs() -> None:
    cfg = make_cfg()
    for momentum in (-0.01, -0.001, 0.0, 0.001, 0.01):
        for vol in (0.0, 1.0, 10.0, 1_000.0):
            for position in (-60.0, -49.0, 0.0, 49.0, 60.0):
                mm = MarketMaker(cfg)
                sig = mm.on_snapshot(
                    make_snap(microprice=42.5, momentum_5s=momentum,
                              realized_vol_bps=vol, ofi_1s=0.5),
                    position=position,
                )
                assert sig is not None
                assert math.isfinite(sig.fair_price) and sig.fair_price > 0.0
                if sig.bid_price is not None:
                    assert sig.bid_price > 0.0
                    assert math.isfinite(sig.bid_price)
                    assert sig.bid_qty >= MIN_QTY
                if sig.ask_price is not None:
                    assert sig.ask_price > 0.0
                    assert math.isfinite(sig.ask_price)
                    assert sig.ask_qty >= MIN_QTY
                if sig.bid_price is not None and sig.ask_price is not None:
                    assert sig.bid_price < sig.ask_price


def test_position_is_not_retained_between_calls() -> None:
    mm = make_mm()
    assert mm.on_snapshot(make_snap(ts_ms=1_000), position=40.0) is not None
    # Position is supplied by the caller on every call; with 0 it quotes
    # symmetrically again.
    sig = mm.on_snapshot(make_snap(ts_ms=1_100), position=0.0)
    assert sig is not None
    assert sig.bid_price == pytest.approx(100.0 * (1 - 0.8 / 1e4), rel=1e-12)
    assert sig.ask_price == pytest.approx(100.0 * (1 + 0.8 / 1e4), rel=1e-12)


def test_stats_is_a_copy() -> None:
    mm = make_mm()
    mm.on_snapshot(make_snap(), position=0.0)
    stats = mm.stats()
    stats["signals"] = 999
    stats["last_signal_ts_ms"]["BTCUSDT"] = 123
    assert mm.stats()["signals"] == 1
    assert mm.stats()["last_signal_ts_ms"]["BTCUSDT"] == 1_000


@pytest.mark.parametrize(
    "bad",
    [
        {"quote_notional_usd": 0.0},
        {"quote_notional_usd": float("nan")},
        {"max_position_notional_usd": -1.0},
        {"inventory_skew": -0.1},
        {"min_half_spread_bps": -1.0},
        {"max_half_spread_bps": 0.1, "min_half_spread_bps": 1.0},
        {"quote_cooldown_ms": -1},
    ],
)
def test_invalid_configuration_is_rejected(bad: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        MarketMaker(make_cfg(**bad))
