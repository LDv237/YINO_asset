"""Per-symbol aggregate-trade ID continuity tracking.

Binance's ``@aggTrade`` stream carries an aggregate trade ID ``a``.  The depth
stream has an explicit U/u sequence check; trades previously had no equivalent
integrity check.  This tracker detects duplicates, backwards events and gaps.

Policy:
* duplicate / backwards event: count and do not feed factors/execution again;
* gap: count the missing IDs but still process the gap event (we do not want to
  discard real market data just because the exchange/connection skipped IDs);
* first event: establishes the baseline, no gap is reported.

A gap can be caused by a reconnect; it is not the same thing as losing a raw
WebSocket frame, and the report must keep those two statements separate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tickforge.events import TradeEvent


@dataclass(slots=True)
class _SymbolTradeState:
    last_id: int | None = None
    duplicates: int = 0
    backwards: int = 0
    gaps: int = 0
    missing_ids: int = 0
    accepted: int = 0


@dataclass(slots=True)
class TradeQualityTracker:
    """Track ``aggTrade.a`` continuity independently per symbol."""

    states: dict[str, _SymbolTradeState] = field(default_factory=dict)

    def ensure(self, symbol: str) -> None:
        """Create empty state so reports include symbols with no trades yet."""
        self._state(symbol)

    def _state(self, symbol: str) -> _SymbolTradeState:
        state = self.states.get(symbol)
        if state is None:
            state = _SymbolTradeState()
            self.states[symbol] = state
        return state

    def observe(self, ev: TradeEvent) -> tuple[str, int]:
        """Return ``(status, missing_ids)`` for one trade event.

        ``status`` is ``first|ok|gap|duplicate|backwards``.  ``missing_ids`` is
        non-zero only for a gap.
        """

        state = self._state(ev.symbol)
        trade_id = ev.agg_trade_id
        if state.last_id is None:
            state.last_id = trade_id
            state.accepted += 1
            return "first", 0
        if trade_id == state.last_id:
            state.duplicates += 1
            return "duplicate", 0
        if trade_id < state.last_id:
            state.backwards += 1
            return "backwards", 0
        if trade_id > state.last_id + 1:
            missing = trade_id - state.last_id - 1
            state.gaps += 1
            state.missing_ids += missing
            state.last_id = trade_id
            state.accepted += 1
            return "gap", missing
        state.last_id = trade_id
        state.accepted += 1
        return "ok", 0

    def stats(self) -> dict[str, dict[str, int | None]]:
        return {
            symbol: {
                "last_agg_trade_id": state.last_id,
                "accepted": state.accepted,
                "duplicates": state.duplicates,
                "backwards": state.backwards,
                "gaps": state.gaps,
                "missing_ids": state.missing_ids,
            }
            for symbol, state in sorted(self.states.items())
        }


__all__ = ["TradeQualityTracker"]
