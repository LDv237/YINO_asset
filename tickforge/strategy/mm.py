"""Deterministic demo market-making strategy.

The strategy is intentionally small: it turns one :class:`FactorSnapshot`
plus the current base-asset position into a two-sided :class:`Signal`.  It is
stateless apart from a per-symbol cooldown timestamp, so replaying the same
snapshots reproduces byte-identical signals.

Formulas (see ``docs/INTERFACES.md`` section 5)::

    ofi_norm    = clamp(ofi_1s / max(bid_qty + ask_qty, 1e-9), -3, 3)
    fair        = microprice * (1 + momentum_coeff * momentum_5s
                                  + ofi_coeff_bps * 1e-4 * ofi_norm)
    target_base = max_position_notional_usd / fair
    reservation = clamp(fair * (1 - inventory_skew * position / target_base),
                        0.5 * fair, 1.5 * fair)
    half_bps    = clamp(min_half_spread_bps
                        + vol_half_spread_coeff * realized_vol_bps,
                        min_half_spread_bps, max_half_spread_bps)
    bid         = reservation * (1 - half_bps / 1e4)
    ask         = reservation * (1 + half_bps / 1e4)
    qty         = max(round(quote_notional_usd / fair, 8), 1e-8)

Quantities are rounded to one tick (1e-8).  No wall clock, randomness or
process-wide state is consulted.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from tickforge.config import StrategyConfig
from tickforge.events import FactorSnapshot, Signal

_LOG = logging.getLogger(__name__)

#: Smallest quantity the demo can quote (one tick).
MIN_QTY = 1e-8

#: OFI normalisation is clamped to this symmetric range.
_MAX_OFI_NORM = 3.0

#: Reservation price is clamped to this band around fair value.
_RESERVATION_MIN_FACTOR = 0.5
_RESERVATION_MAX_FACTOR = 1.5


def _clamp(value: float, low: float, high: float) -> float:
    """Return ``value`` clamped to ``[low, high]`` (finite inputs assumed)."""
    if value < low:
        return low
    if value > high:
        return high
    return value


def _finite_or_zero(value: float) -> float:
    """Return ``value`` when finite, else ``0.0``.

    Factor outputs are documented to be finite; this is a defensive guard so a
    malformed upstream snapshot can never leak NaN/Inf into a signal.
    """
    return value if math.isfinite(value) else 0.0


class MarketMaker:
    """Deterministic inventory-aware market maker used by the demo pipeline.

    Parameters
    ----------
    cfg:
        Strategy parameters.  The instance keeps a reference to the config;
        callers must not mutate it while the strategy is running.
    logger:
        Optional logger; defaults to the module logger.

    The class keeps no position state.  The caller passes the current position
    on every call, which makes the mapping snapshot -> signal a pure function
    of (snapshot, position) apart from the per-symbol cooldown timestamp.
    """

    def __init__(
        self, cfg: StrategyConfig, *, logger: logging.Logger | None = None
    ) -> None:
        self._validate(cfg)
        self._cfg = cfg
        self._log = logger if logger is not None else _LOG
        self._allow_short = bool(cfg.allow_short)
        # Cooldown is per symbol: a shared timer would starve other symbols.
        self._last_signal_ts_ms: dict[str, int] = {}
        self._stats: dict[str, Any] = {
            "snapshots": 0,
            "signals": 0,
            "disabled": 0,
            "cooldown_suppressed": 0,
            "invalid": 0,
            "bid_skipped": 0,
            "ask_skipped": 0,
        }

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate(cfg: StrategyConfig) -> None:
        if not math.isfinite(cfg.quote_notional_usd) or cfg.quote_notional_usd <= 0:
            raise ValueError("quote_notional_usd must be a positive finite number")
        if (
            not math.isfinite(cfg.max_position_notional_usd)
            or cfg.max_position_notional_usd <= 0
        ):
            raise ValueError("max_position_notional_usd must be a positive finite number")
        if not math.isfinite(cfg.inventory_skew) or cfg.inventory_skew < 0:
            raise ValueError("inventory_skew must be a non-negative finite number")
        for name in (
            "ofi_coeff_bps",
            "momentum_coeff",
            "min_half_spread_bps",
            "vol_half_spread_coeff",
            "max_half_spread_bps",
        ):
            value = getattr(cfg, name)
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        if cfg.min_half_spread_bps < 0:
            raise ValueError("min_half_spread_bps must be >= 0")
        if cfg.max_half_spread_bps < cfg.min_half_spread_bps:
            raise ValueError("max_half_spread_bps must be >= min_half_spread_bps")
        if cfg.quote_cooldown_ms < 0:
            raise ValueError("quote_cooldown_ms must be >= 0")

    # ------------------------------------------------------------------
    # Signal generation
    # ------------------------------------------------------------------
    def on_snapshot(
        self, snap: FactorSnapshot, *, position: float
    ) -> Signal | None:
        """Produce a quote proposal for ``snap`` given the current position.

        Returns ``None`` when the strategy is disabled, the snapshot cannot be
        priced safely (non-positive/non-finite fair price), the position is not
        finite, or the per-symbol cooldown has not elapsed yet.

        A signal is still returned when one side is suppressed by the
        inventory limit; that side then has ``*_price=None`` and ``*_qty=0``.
        """
        stats = self._stats
        stats["snapshots"] += 1

        if not self._cfg.enabled:
            stats["disabled"] += 1
            return None

        if not math.isfinite(position):
            stats["invalid"] += 1
            return None

        microprice = snap.microprice
        if not math.isfinite(microprice) or microprice <= 0.0:
            stats["invalid"] += 1
            return None

        momentum = _finite_or_zero(snap.momentum_5s)
        ofi = _finite_or_zero(snap.ofi_1s)
        qty_sum = snap.bid_qty + snap.ask_qty
        if not math.isfinite(qty_sum) or qty_sum < 0.0:
            qty_sum = 0.0
        ofi_norm = _clamp(ofi / max(qty_sum, 1e-9), -_MAX_OFI_NORM, _MAX_OFI_NORM)

        cfg = self._cfg
        fair = microprice * (
            1.0
            + cfg.momentum_coeff * momentum
            + cfg.ofi_coeff_bps * 1e-4 * ofi_norm
        )
        if not math.isfinite(fair) or fair <= 0.0:
            stats["invalid"] += 1
            return None

        last_ts = self._last_signal_ts_ms.get(snap.symbol)
        if (
            last_ts is not None
            and snap.ts_ms - last_ts < cfg.quote_cooldown_ms
        ):
            stats["cooldown_suppressed"] += 1
            return None

        vol_bps = _finite_or_zero(snap.realized_vol_bps)
        if vol_bps < 0.0:
            vol_bps = 0.0
        half_bps = _clamp(
            cfg.min_half_spread_bps + cfg.vol_half_spread_coeff * vol_bps,
            cfg.min_half_spread_bps,
            cfg.max_half_spread_bps,
        )

        target_base = cfg.max_position_notional_usd / fair
        if not math.isfinite(target_base) or target_base <= 0.0:
            stats["invalid"] += 1
            return None

        reservation = fair * (1.0 - cfg.inventory_skew * position / target_base)
        if not math.isfinite(reservation):
            stats["invalid"] += 1
            return None
        reservation = _clamp(
            reservation,
            _RESERVATION_MIN_FACTOR * fair,
            _RESERVATION_MAX_FACTOR * fair,
        )

        qty = cfg.quote_notional_usd / fair
        if not math.isfinite(qty) or qty <= 0.0:
            stats["invalid"] += 1
            return None
        qty = max(round(qty, 8), MIN_QTY)

        bid_price = reservation * (1.0 - half_bps / 1e4)
        ask_price = reservation * (1.0 + half_bps / 1e4)

        # Inventory limit: do not add exposure on the side that is already at
        # the configured maximum.  In spot long-only mode, never quote an ask
        # without base inventory, and never sell more than the current holding.
        quote_bid = position < target_base
        ask_qty = qty
        if self._allow_short:
            quote_ask = position > -target_base
        else:
            quote_ask = position > 0.0
            # Floor, not round: 5e-9 must not become a 1e-8 quote, otherwise
            # the simulator would be asked to sell inventory we do not hold.
            ask_qty = math.floor(min(qty, max(0.0, position)) * 1e8) / 1e8
            if ask_qty < MIN_QTY:
                quote_ask = False

        if quote_bid and not (math.isfinite(bid_price) and bid_price > 0.0):
            quote_bid = False
        if quote_ask and not (math.isfinite(ask_price) and ask_price > 0.0):
            quote_ask = False

        if not quote_bid and not quote_ask:
            stats["invalid"] += 1
            stats["bid_skipped"] += 1
            stats["ask_skipped"] += 1
            return None

        self._last_signal_ts_ms[snap.symbol] = snap.ts_ms
        stats["signals"] += 1
        if not quote_bid:
            stats["bid_skipped"] += 1
        if not quote_ask:
            stats["ask_skipped"] += 1

        return Signal(
            symbol=snap.symbol,
            ts_ms=snap.ts_ms,
            seq=snap.seq,
            fair_price=fair,
            bid_price=bid_price if quote_bid else None,
            bid_qty=qty if quote_bid else 0.0,
            ask_price=ask_price if quote_ask else None,
            ask_qty=ask_qty if quote_ask else 0.0,
            reason="mm",
        )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def stats(self) -> dict[str, Any]:
        """Return a copy of cumulative strategy counters.

        Keys: ``snapshots, signals, disabled, cooldown_suppressed, invalid,
        bid_skipped, ask_skipped`` plus a copy of the per-symbol cooldown
        timestamps under ``last_signal_ts_ms``.
        """
        out = dict(self._stats)
        out["last_signal_ts_ms"] = dict(self._last_signal_ts_ms)
        return out


__all__ = ["MarketMaker", "MIN_QTY"]
