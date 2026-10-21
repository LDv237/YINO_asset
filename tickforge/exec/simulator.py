"""Deterministic simulated maker execution for the TickForge demo.

The simulator owns all mutable trading state (resting quotes, position, cash,
average cost).  It never consults the wall clock or a random generator, so the
same ``Signal``/``TradeEvent`` sequence produces the same ``Fill`` objects.

Model
-----
* ``apply_signal`` replaces both resting quotes of a symbol.  A resting quote
  keeps a *remaining* quantity which is reduced by partial fills.
* ``on_trade`` checks the resting bid first and then the resting ask.  A buy
  fills when ``trade.price <= bid``; a sell fills when ``trade.price >= ask``.
  Both sides may fill from the same trade (buy first, fixed order).
* Fill quantity is ``min(remaining_quote_qty, trade_qty)`` further capped so
  that ``abs(new_position) * fill_price <= max_position_notional_usd``.
  A capped fill increments ``risk_truncated``; a completely rejected one
  increments ``risk_rejected``.
* Average-cost accounting.  ``avg_cost`` is the pure average entry price of
  the open position.  Realised PnL is cumulative and fee-aware: every fee is
  recorded in ``cash`` and also reflected in ``realized_pnl`` (opening fees
  immediately, closing fees in the realised term).  Consequently::

      total_pnl == realized_pnl + unrealized_pnl == cash + position * mark
      unrealized_pnl == (mark - avg_cost) * position          (signed)

  where a short position has negative ``position``.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

from tickforge.config import StrategyConfig
from tickforge.events import TICKS_PER_UNIT, Fill, Signal, TradeEvent

_LOG = logging.getLogger(__name__)

# One base tick (1e-8) times one quote tick (1e-8) is 1e-16 quote units.
_NOTIONAL_SCALE = 10**16


@dataclass(slots=True)
class _SymbolState:
    """Mutable per-symbol execution state."""

    position_ticks: int = 0
    avg_cost: float = 0.0
    cash: float = 0.0
    initial_cash: float = 0.0
    realized_pnl: float = 0.0
    fees: float = 0.0
    fills: int = 0
    max_abs_position_ticks: int = 0
    mark_price: float | None = None
    trades: int = 0
    signals: int = 0
    risk_truncated: int = 0
    risk_rejected: int = 0
    bid_price_ticks: int | None = None
    bid_qty_ticks: int = 0
    ask_price_ticks: int | None = None
    ask_qty_ticks: int = 0
    bid_ts_ms: int | None = None
    ask_ts_ms: int | None = None


class Simulator:
    """In-memory maker execution simulator.

    Semantics
    ---------
    This is an abstract signal-to-fill ledger, **not** a Binance spot account
    model.  Negative base positions and negative cash are allowed on purpose so
    that a two-sided market maker can run from zero initial inventory; borrowing
    costs, margin, liquidation and exchange settlement are not modelled.  A
    negative position here does not imply that the spot account could sell
    inventory it does not hold.

    Parameters
    ----------
    cfg:
        Strategy parameters.  Only ``fee_bps`` and
        ``max_position_notional_usd`` influence execution.
    symbols:
        Optional symbols whose (zeroed) state should exist in ``summary()``
        before any signal or trade arrives.
    logger:
        Optional logger; defaults to the module logger.
    """

    def __init__(
        self,
        cfg: StrategyConfig,
        *,
        symbols: list[str] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not math.isfinite(cfg.fee_bps) or cfg.fee_bps < 0:
            raise ValueError("fee_bps must be a non-negative finite number")
        if (
            not math.isfinite(cfg.max_position_notional_usd)
            or cfg.max_position_notional_usd <= 0
        ):
            raise ValueError("max_position_notional_usd must be a positive finite number")

        self._cfg = cfg
        self._log = logger if logger is not None else _LOG
        #: Fee rate as a fraction of notional (bps / 1e4).
        self._fee_rate = cfg.fee_bps / 10_000.0
        #: Position limit expressed in quote units of 1e-16 USD.
        self._max_notional_ticks2 = int(
            round(cfg.max_position_notional_usd * _NOTIONAL_SCALE)
        )
        if self._max_notional_ticks2 <= 0:
            raise ValueError("max_position_notional_usd is smaller than one tick")
        if not math.isfinite(cfg.initial_cash_usd) or cfg.initial_cash_usd < 0:
            raise ValueError("initial_cash_usd must be a finite non-negative number")
        self._allow_short = bool(cfg.allow_short)
        self._initial_cash = float(cfg.initial_cash_usd)

        self._states: dict[str, _SymbolState] = {}
        for symbol in symbols or ():
            if symbol not in self._states:
                self._states[symbol] = self._new_state()

    # ------------------------------------------------------------------
    # Quote management
    # ------------------------------------------------------------------
    @staticmethod
    def _to_quote_ticks(
        price: float | None, qty: float
    ) -> tuple[int | None, int]:
        """Convert a signal price/quantity to integer ticks.

        Returns ``(None, 0)`` for absent, non-positive or non-finite values.
        Quantities below half a tick collapse to ``0`` and are therefore not
        quoted.
        """
        if price is None or qty is None:
            return None, 0
        if (
            not math.isfinite(price)
            or price <= 0.0
            or not math.isfinite(qty)
            or qty <= 0.0
        ):
            return None, 0
        scaled_price = price * TICKS_PER_UNIT
        scaled_qty = qty * TICKS_PER_UNIT
        # Finite inputs can still overflow after scaling (e.g. 1e305 * 1e8);
        # drop them instead of raising OverflowError on round().
        if (
            not math.isfinite(scaled_price)
            or not math.isfinite(scaled_qty)
            or scaled_price <= 0.0
            or scaled_qty <= 0.0
        ):
            return None, 0
        price_ticks = int(round(scaled_price))
        qty_ticks = int(round(scaled_qty))
        if price_ticks <= 0 or qty_ticks <= 0:
            return None, 0
        return price_ticks, qty_ticks

    def apply_signal(self, sig: Signal) -> None:
        """Replace the resting bid and ask for ``sig.symbol``.

        Any previous quotes are discarded even when the new signal has no
        price on one side, which mirrors "cancel/replace" semantics.
        """
        state = self._state(sig.symbol)
        state.bid_price_ticks, state.bid_qty_ticks = self._to_quote_ticks(
            sig.bid_price, sig.bid_qty
        )
        state.ask_price_ticks, state.ask_qty_ticks = self._to_quote_ticks(
            sig.ask_price, sig.ask_qty
        )
        state.bid_ts_ms = int(sig.ts_ms)
        state.ask_ts_ms = int(sig.ts_ms)
        state.signals += 1

    def cancel(self, symbol: str) -> None:
        """Remove all resting quotes for ``symbol``; position is untouched."""
        state = self._state(symbol)
        state.bid_price_ticks = None
        state.bid_qty_ticks = 0
        state.ask_price_ticks = None
        state.ask_qty_ticks = 0
        state.bid_ts_ms = None
        state.ask_ts_ms = None

    def mark(self, symbol: str, mid: float) -> None:
        """Record a mark price used for unrealised PnL.

        Non-finite or non-positive marks are ignored (an empty book must not
        poison the summary).  The previous mark stays in effect.
        """
        if not math.isfinite(mid) or mid <= 0.0:
            return
        self._state(symbol).mark_price = mid

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def on_trade(self, ev: TradeEvent) -> list[Fill]:
        """Check resting quotes against ``ev`` and return fills in fixed order.

        The bid side is always evaluated before the ask side.  At most one fill
        per side is produced per trade event.
        """
        state = self._state(ev.symbol)
        state.trades += 1

        price_ticks = ev.price_ticks
        if price_ticks <= 0 or ev.qty_ticks <= 0:
            return []

        fills: list[Fill] = []
        trade_ts = ev.effective_trade_time_ms
        bid_visible = state.bid_ts_ms is None or trade_ts >= state.bid_ts_ms
        ask_visible = state.ask_ts_ms is None or trade_ts >= state.ask_ts_ms
        if (
            bid_visible
            and state.bid_price_ticks is not None
            and price_ticks <= state.bid_price_ticks
        ):
            fill = self._try_fill(state, ev, side="BUY", price_ticks=state.bid_price_ticks)
            if fill is not None:
                fills.append(fill)
        if (
            ask_visible
            and state.ask_price_ticks is not None
            and price_ticks >= state.ask_price_ticks
        ):
            fill = self._try_fill(state, ev, side="SELL", price_ticks=state.ask_price_ticks)
            if fill is not None:
                fills.append(fill)
        return fills

    def _try_fill(
        self,
        state: _SymbolState,
        ev: TradeEvent,
        *,
        side: str,
        price_ticks: int,
    ) -> Fill | None:
        """Execute at most one fill for one quote side, or return ``None``."""
        if side == "BUY":
            remaining = state.bid_qty_ticks
        else:
            remaining = state.ask_qty_ticks
        want_ticks = min(remaining, ev.qty_ticks)
        if want_ticks <= 0:
            return None

        # Exact integer notional check: max base ticks at this quote price.
        max_ticks_at_price = self._max_notional_ticks2 // price_ticks
        if side == "BUY":
            allowed_ticks = max_ticks_at_price - state.position_ticks
        else:
            allowed_ticks = max_ticks_at_price + state.position_ticks

        if not self._allow_short:
            # Spot long-only: buys are limited by the quote cash balance
            # (including the fee buffer); sells are limited by inventory.
            if side == "BUY":
                price = price_ticks / TICKS_PER_UNIT
                cash_ticks = int(
                    max(0.0, state.cash)
                    / (price * (1.0 + self._fee_rate))
                    * TICKS_PER_UNIT
                )
                allowed_ticks = min(allowed_ticks, cash_ticks)
            else:
                allowed_ticks = min(allowed_ticks, max(0, state.position_ticks))

        if allowed_ticks <= 0:
            state.risk_rejected += 1
            return None

        qty_ticks = min(want_ticks, allowed_ticks)
        if qty_ticks < want_ticks:
            state.risk_truncated += 1

        price = price_ticks / TICKS_PER_UNIT
        qty = qty_ticks / TICKS_PER_UNIT
        fee = price * qty * self._fee_rate
        if not math.isfinite(fee):
            state.risk_rejected += 1
            return None

        if side == "BUY":
            self._apply_buy(state, price=price, qty=qty, qty_ticks=qty_ticks, fee=fee)
            state.bid_qty_ticks -= qty_ticks
            if state.bid_qty_ticks <= 0:
                state.bid_price_ticks = None
                state.bid_qty_ticks = 0
        else:
            self._apply_sell(state, price=price, qty=qty, qty_ticks=qty_ticks, fee=fee)
            state.ask_qty_ticks -= qty_ticks
            if state.ask_qty_ticks <= 0:
                state.ask_price_ticks = None
                state.ask_qty_ticks = 0

        position = state.position_ticks / TICKS_PER_UNIT
        state.max_abs_position_ticks = max(
            state.max_abs_position_ticks, abs(state.position_ticks)
        )
        return Fill(
            symbol=ev.symbol,
            ts_ms=ev.effective_trade_time_ms,
            trade_id=ev.agg_trade_id,
            side=side,
            price=price,
            qty=qty,
            fee=fee,
            position=position,
            cash=state.cash,
            realized_pnl=state.realized_pnl,
            reason="maker",
        )

    @staticmethod
    def _apply_buy(
        state: _SymbolState, *, price: float, qty: float, qty_ticks: int, fee: float
    ) -> None:
        """Update position/avg cost/realised PnL for a filled buy."""
        old_position = state.position_ticks / TICKS_PER_UNIT
        old_avg_cost = state.avg_cost
        state.position_ticks += qty_ticks
        new_position = state.position_ticks / TICKS_PER_UNIT

        if old_position >= 0.0:
            # Opening or adding to a long: weighted average entry price.
            if state.position_ticks != 0:
                state.avg_cost = (
                    old_avg_cost * old_position + price * qty
                ) / new_position
            state.realized_pnl -= fee
        else:
            # Covering a short (possibly crossing through zero into a long).
            covered = min(qty, -old_position)
            state.realized_pnl += (old_avg_cost - price) * covered - fee
            if qty > -old_position:
                state.avg_cost = price
            elif state.position_ticks == 0:
                state.avg_cost = 0.0

        state.cash -= price * qty + fee
        state.fees += fee
        state.fills += 1

    @staticmethod
    def _apply_sell(
        state: _SymbolState, *, price: float, qty: float, qty_ticks: int, fee: float
    ) -> None:
        """Update position/avg cost/realised PnL for a filled sell."""
        old_position = state.position_ticks / TICKS_PER_UNIT
        old_avg_cost = state.avg_cost
        state.position_ticks -= qty_ticks
        new_position = state.position_ticks / TICKS_PER_UNIT

        if old_position <= 0.0:
            # Opening or adding to a short: weighted average entry price.
            if new_position != 0.0:
                state.avg_cost = (
                    old_avg_cost * (-old_position) + price * qty
                ) / (-new_position)
            else:  # pragma: no cover - a sell cannot flatten a short
                state.avg_cost = 0.0
            state.realized_pnl -= fee
        else:
            # Closing a long (possibly crossing through zero into a short).
            realized_qty = min(qty, old_position)
            state.realized_pnl += (price - old_avg_cost) * realized_qty - fee
            if qty > old_position:
                state.avg_cost = price
            elif state.position_ticks == 0:
                state.avg_cost = 0.0

        state.cash += price * qty - fee
        state.fees += fee
        state.fills += 1

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def position(self, symbol: str) -> float:
        """Return the current base-asset position (signed)."""
        state = self._states.get(symbol)
        if state is None:
            return 0.0
        return state.position_ticks / TICKS_PER_UNIT

    def _new_state(self) -> _SymbolState:
        return _SymbolState(
            cash=self._initial_cash,
            initial_cash=self._initial_cash,
        )

    def _state(self, symbol: str) -> _SymbolState:
        state = self._states.get(symbol)
        if state is None:
            state = self._new_state()
            self._states[symbol] = state
        return state

    def _symbol_summary(self, symbol: str, state: _SymbolState) -> dict[str, Any]:
        position = state.position_ticks / TICKS_PER_UNIT
        if state.mark_price is not None:
            mark = state.mark_price
        elif position != 0.0:
            # Without an explicit mark, value the position at its entry price
            # so unrealised PnL is exactly zero rather than undefined.
            mark = state.avg_cost
        else:
            mark = 0.0

        unrealized = (mark - state.avg_cost) * position
        total = state.cash + position * mark - state.initial_cash
        open_bid = (
            state.bid_price_ticks / TICKS_PER_UNIT
            if state.bid_price_ticks is not None
            else None
        )
        open_ask = (
            state.ask_price_ticks / TICKS_PER_UNIT
            if state.ask_price_ticks is not None
            else None
        )
        return {
            "symbol": symbol,
            "position": position,
            "avg_cost": state.avg_cost if position != 0.0 else 0.0,
            "mark": mark,
            "cash": state.cash,
            "initial_cash": state.initial_cash,
            "realized_pnl": state.realized_pnl,
            "unrealized_pnl": unrealized,
            "total_pnl": total,
            "fees": state.fees,
            "fills": state.fills,
            "open_bid": open_bid,
            "open_ask": open_ask,
            "open_bid_qty": state.bid_qty_ticks / TICKS_PER_UNIT,
            "open_ask_qty": state.ask_qty_ticks / TICKS_PER_UNIT,
            "max_abs_position": state.max_abs_position_ticks / TICKS_PER_UNIT,
            "trades": state.trades,
            "signals": state.signals,
            "risk_truncated": state.risk_truncated,
            "risk_rejected": state.risk_rejected,
        }

    def summary(self) -> dict[str, Any]:
        """Return per-symbol accounting plus aggregate totals.

        Structure::

            {"symbols": {symbol: {...}}, "totals": {...}}

        Per-symbol keys are ``position, avg_cost, mark, cash, realized_pnl,
        unrealized_pnl, total_pnl, fees, fills, open_bid, open_ask,
        open_bid_qty, open_ask_qty, max_abs_position, trades, signals,
        risk_truncated, risk_rejected``.

        Totals sum the same numeric fields (``avg_cost`` is weighted by
        absolute position; ``open_bid``/``open_ask`` are resting *notional*
        while their ``*_qty`` variants sum base quantity).  Symbols are emitted
        in sorted order so the summary is deterministic.
        """
        symbols: dict[str, dict[str, Any]] = {}
        for symbol in sorted(self._states):
            symbols[symbol] = self._symbol_summary(symbol, self._states[symbol])

        totals = {
            "position": 0.0,
            "avg_cost": 0.0,
            "cash": 0.0,
            "initial_cash": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "total_pnl": 0.0,
            "fees": 0.0,
            "fills": 0,
            "open_bid": 0.0,
            "open_ask": 0.0,
            "open_bid_qty": 0.0,
            "open_ask_qty": 0.0,
            "max_abs_position": 0.0,
            "trades": 0,
            "signals": 0,
            "risk_truncated": 0,
            "risk_rejected": 0,
        }
        weighted_cost = 0.0
        abs_position = 0.0
        for row in symbols.values():
            for key in (
                "position",
                "cash",
                "initial_cash",
                "realized_pnl",
                "unrealized_pnl",
                "total_pnl",
                "fees",
                "open_bid_qty",
                "open_ask_qty",
                "max_abs_position",
            ):
                totals[key] += row[key]
            for key in ("fills", "trades", "signals", "risk_truncated", "risk_rejected"):
                totals[key] += row[key]
            if row["open_bid"] is not None:
                totals["open_bid"] += row["open_bid"] * row["open_bid_qty"]
            if row["open_ask"] is not None:
                totals["open_ask"] += row["open_ask"] * row["open_ask_qty"]
            weighted_cost += row["avg_cost"] * abs(row["position"])
            abs_position += abs(row["position"])

        totals["avg_cost"] = weighted_cost / abs_position if abs_position > 0.0 else 0.0
        return {"symbols": symbols, "totals": totals}


__all__ = ["Simulator"]
