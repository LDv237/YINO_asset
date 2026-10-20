"""Per-symbol microstructure factor engine.

The engine consumes the *post-update* state of a synchronised
:class:`~tickforge.book.orderbook.OrderBook` and periodically produces
:class:`~tickforge.events.FactorSnapshot` values.

Design notes
------------
* Every symbol owns an independent state object.  Each rolling window is a
  ``deque`` of ``(event_time_ms, value)`` entries purged lazily when time
  advances, which makes updates amortised O(1) and bounds memory by the
  configured time windows rather than by process lifetime.
* ``on_depth`` is called *after* the book has applied the diff event.  The
  engine remembers the previous visible top of book itself; that is what makes
  the order-flow imbalance (OFI) comparable across updates and what lets
  :meth:`FactorEngine.reset_book_state` break the chain after a resync.
* Event time never moves backwards.  The depth and aggTrade streams carry
  independent exchange clocks, so each is advanced with
  ``max(previous, event.event_time_ms)``.  The emission throttle and the
  snapshot ``ts_ms`` use the depth clock only; out-of-order depth timestamps
  can therefore never move a snapshot backwards or re-open a throttle.
* Only event content and the resulting book state feed the factors.  No wall
  clock, randomness or process-wide state is consulted, so WAL replay is
  byte-for-byte reproducible.
* All public outputs are finite floats.  Every division is guarded; when a
  denominator is zero the documented neutral value (usually ``0.0``) is used.

Window semantics are half-open at the old end, ``(now - window, now]``: an
observation exactly one window old is still included.  This matches the
"at least 4 s of history" rule used for momentum.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from ..book.orderbook import BestQuote, OrderBook
from ..events import TICKS_PER_UNIT, DepthEvent, FactorSnapshot, TradeEvent

#: Number of book levels used by ``depth_imbalance_5``.
_DEPTH_LEVELS = 5

#: Default momentum requires at least 4 s of observations (5 s window minus a
#: 1 s safety margin) to avoid extrapolating from a very short history.
_MOMENTUM_MIN_HISTORY_MS = 4_000


@dataclass(slots=True)
class _SymbolState:
    """All mutable factor state for one symbol.

    Book-derived fields are cleared by
    :meth:`FactorEngine.reset_book_state`; trade-window fields are kept, as
    required by the interface contract.
    """

    depth_updates: int = 0
    trades: int = 0
    snapshots: int = 0
    #: Monotonic high-water marks, kept separately because the depth and
    #: aggTrade streams have independent event clocks.  Neither ever decreases.
    depth_ts_ms: int = 0
    trade_ts_ms: int = 0
    #: ``ts_ms`` of the last emitted snapshot; ``None`` until the first one.
    last_emit_ts_ms: int | None = None

    # -- book-derived state -------------------------------------------------
    prev_quote: BestQuote | None = None
    ofi_events: deque[tuple[int, float]] = field(default_factory=deque)
    ofi_sum: float = 0.0
    mids: deque[tuple[int, float]] = field(default_factory=deque)
    prev_mid: float | None = None
    ewma_var: float = 0.0

    # -- trade-derived state (survives reset_book_state) ---------------------
    # Each entry is (ts_ms, signed_qty): positive for buyer-initiated trades,
    # negative for seller-initiated ones.  ``trade_gross`` is the sum of
    # absolute quantities, so imbalance = signed / gross.
    trade_events: deque[tuple[int, float]] = field(default_factory=deque)
    trade_signed: float = 0.0
    trade_gross: float = 0.0


def _finite_or(value: float, fallback: float = 0.0) -> float:
    """Return ``value`` if finite, else ``fallback``."""
    return value if math.isfinite(value) else fallback


def _clamp_unit(value: float) -> float:
    """Clamp an imbalance to ``[-1.0, 1.0]`` (guards float round-off)."""
    if value > 1.0:
        return 1.0
    if value < -1.0:
        return -1.0
    return value


def _ofi_delta(prev: BestQuote, cur: BestQuote) -> int:
    """Classic top-of-book OFI contribution between two adjacent quotes.

    Returns the signed quantity change in integer ticks:

    * bid price up -> ``+cur.bid_qty``; unchanged -> quantity difference;
      down -> ``-prev.bid_qty``;
    * ask price down -> ``-cur.ask_qty``; unchanged -> *reversed* quantity
      difference; up -> ``+prev.ask_qty``.

    This is the Cont-Kukanov-Stoikov sign convention: a lower ask means new
    sell-side liquidity (negative pressure), while an ask that moves up means
    the previous ask was removed (positive pressure).  A positive value means
    net buying pressure.
    """
    delta = 0

    if cur.bid_px > prev.bid_px:
        delta += cur.bid_qty
    elif cur.bid_px == prev.bid_px:
        delta += cur.bid_qty - prev.bid_qty
    else:
        delta -= prev.bid_qty

    if cur.ask_px < prev.ask_px:
        delta -= cur.ask_qty
    elif cur.ask_px == prev.ask_px:
        delta -= cur.ask_qty - prev.ask_qty
    else:
        delta += prev.ask_qty

    return delta


def _require_int(name: str, value: object, minimum: int) -> int:
    """Validate an integer constructor argument and return it."""
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


class FactorEngine:
    """Deterministic per-symbol rolling factor computation.

    Parameters
    ----------
    interval_ms:
        Minimum event-time distance between emitted snapshots for a symbol.
        The first snapshot after construction or a reset is emitted
        immediately.  ``0`` emits on every depth update.
    ofi_window_ms:
        Rolling window for the order-flow imbalance sum.
    trade_window_ms:
        Rolling window for the trade-imbalance sum.
    momentum_window_ms:
        Rolling window of mid prices used for momentum.
    vol_alpha:
        EWMA decay for the squared log-return variance, in ``[0, 1)``.

    All four windows are evaluated on exchange event time, never wall time.
    """

    def __init__(
        self,
        *,
        interval_ms: int = 100,
        ofi_window_ms: int = 1000,
        trade_window_ms: int = 1000,
        momentum_window_ms: int = 5000,
        vol_alpha: float = 0.97,
    ) -> None:
        self._interval_ms = _require_int("interval_ms", interval_ms, 0)
        self._ofi_window_ms = _require_int("ofi_window_ms", ofi_window_ms, 1)
        self._trade_window_ms = _require_int("trade_window_ms", trade_window_ms, 1)
        self._momentum_window_ms = _require_int(
            "momentum_window_ms", momentum_window_ms, 1
        )

        try:
            alpha = float(vol_alpha)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise ValueError("vol_alpha must be a finite number in [0, 1)") from exc
        if not math.isfinite(alpha) or not 0.0 <= alpha < 1.0:
            raise ValueError("vol_alpha must be a finite number in [0, 1)")
        self._vol_alpha = alpha

        # A short custom momentum window still requires a full window of
        # history; the default 5 s window therefore requires the documented 4 s.
        self._momentum_min_history_ms = min(
            _MOMENTUM_MIN_HISTORY_MS, self._momentum_window_ms
        )

        self._states: dict[str, _SymbolState] = {}

    # ------------------------------------------------------------------
    # Event ingestion
    # ------------------------------------------------------------------
    def on_depth(self, book: OrderBook, ev: DepthEvent) -> FactorSnapshot | None:
        """Process one applied depth event and maybe emit a snapshot.

        The caller must invoke this *after* ``book.apply_depth(ev)`` succeeded
        for a synchronised book.  The method updates the rolling OFI, mid and
        volatility state on every call, but returns a snapshot only when the
        per-symbol throttle allows it.

        Returns ``None`` when the book is not synchronised, exposes no usable
        two-sided quote, or the throttle interval has not elapsed.
        """
        state = self._state(ev.symbol)
        state.depth_updates += 1

        # Never let an out-of-order depth event move the depth clock
        # backwards.  Trade timestamps are tracked separately because the
        # throttle is defined on depth event time.
        ts_ms = max(int(ev.event_time_ms), state.depth_ts_ms)
        state.depth_ts_ms = ts_ms

        if not book.synced:
            # The caller is expected to reset book state on resync; clearing
            # the chain here is cheap insurance against a fake OFI jump.
            state.prev_quote = None
            return None

        quote = book.best()
        if quote is None:
            # One-sided or otherwise unpriceable top of book.  Break both
            # chains so the next healthy quote does not produce a synthetic
            # OFI/volatility jump across the gap.
            state.prev_quote = None
            state.prev_mid = None
            return None

        self._purge_ofi(state, ts_ms)
        self._purge_mids(state, ts_ms)
        self._purge_trades(state, ts_ms)

        # --- rolling order-flow imbalance ---------------------------------
        if state.prev_quote is not None:
            delta_ticks = _ofi_delta(state.prev_quote, quote)
            if delta_ticks:
                delta = delta_ticks / TICKS_PER_UNIT
                state.ofi_events.append((ts_ms, delta))
                state.ofi_sum += delta
        state.prev_quote = quote

        # --- mid history and EWMA realized volatility ---------------------
        mid = (quote.bid_px + quote.ask_px) / (2.0 * TICKS_PER_UNIT)
        state.mids.append((ts_ms, mid))

        prev_mid = state.prev_mid
        if prev_mid is not None and prev_mid > 0.0 and mid > 0.0:
            log_return = math.log(mid / prev_mid)
            state.ewma_var = (
                self._vol_alpha * state.ewma_var
                + (1.0 - self._vol_alpha) * log_return * log_return
            )
            if not math.isfinite(state.ewma_var):  # pragma: no cover - defensive
                state.ewma_var = 0.0
        state.prev_mid = mid

        # --- throttle ------------------------------------------------------
        last_emit = state.last_emit_ts_ms
        if last_emit is not None and ts_ms - last_emit < self._interval_ms:
            return None

        snapshot = self._build_snapshot(book, state, ev, ts_ms, quote, mid)
        state.last_emit_ts_ms = ts_ms
        state.snapshots += 1
        return snapshot

    def on_trade(self, ev: TradeEvent) -> None:
        """Record one aggressor trade in the symbol's rolling trade window.

        ``is_buyer_maker`` is ``True`` when the buyer was resting, i.e. the
        aggressor was a seller, so the trade counts as sell volume.  No
        snapshot is produced here; trade imbalance is sampled by
        :meth:`on_depth`.
        """
        state = self._state(ev.symbol)
        state.trades += 1

        # Keep the trade window sorted even if aggTrade events arrive out of
        # order; a zero-quantity trade still advances (and purges) the clock.
        ts_ms = max(int(ev.event_time_ms), state.trade_ts_ms)
        state.trade_ts_ms = ts_ms
        self._purge_trades(state, ts_ms)

        qty_ticks = int(ev.qty_ticks)
        if qty_ticks <= 0:
            # No volume and no direction; still counts as a trade event.
            return

        qty = qty_ticks / TICKS_PER_UNIT
        signed = -qty if ev.is_buyer_maker else qty
        state.trade_events.append((ts_ms, signed))
        state.trade_signed += signed
        state.trade_gross += qty

    def reset_book_state(self, symbol: str) -> None:
        """Discard book-derived state for ``symbol`` after a resync.

        Clears the previous best quote, the OFI window, the mid/momentum
        history and the volatility state.  The next :meth:`on_depth` starts
        from a clean slate and emits immediately.  Trade windows and cumulative
        counters are deliberately preserved: trades are independent of book
        synchronisation.
        """
        state = self._states.get(symbol.upper())
        if state is None:
            return

        state.prev_quote = None
        state.last_emit_ts_ms = None
        state.ofi_events.clear()
        state.ofi_sum = 0.0
        state.mids.clear()
        state.prev_mid = None
        state.ewma_var = 0.0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def stats(self) -> dict[str, dict[str, int]]:
        """Return per-symbol counters keyed by upper-case symbol.

        Each value contains ``depth_updates``, ``trades``, ``snapshots`` and
        ``last_ts_ms`` (the later of the depth and trade event-time high-water
        marks).
        """
        return {
            symbol: {
                "depth_updates": state.depth_updates,
                "trades": state.trades,
                "snapshots": state.snapshots,
                "last_ts_ms": max(state.depth_ts_ms, state.trade_ts_ms),
            }
            for symbol, state in sorted(self._states.items())
        }

    # ------------------------------------------------------------------
    # Snapshot construction
    # ------------------------------------------------------------------
    def _build_snapshot(
        self,
        book: OrderBook,
        state: _SymbolState,
        ev: DepthEvent,
        ts_ms: int,
        quote: BestQuote,
        mid: float,
    ) -> FactorSnapshot:
        """Compute all factor values from the current book and windows."""
        bid_px = quote.bid_px
        ask_px = quote.ask_px
        bid_qty_ticks = quote.bid_qty
        ask_qty_ticks = quote.ask_qty

        total_qty_ticks = bid_qty_ticks + ask_qty_ticks
        if total_qty_ticks > 0:
            micro_ticks = (
                bid_px * ask_qty_ticks + ask_px * bid_qty_ticks
            ) / total_qty_ticks
            microprice = micro_ticks / TICKS_PER_UNIT
        else:  # pragma: no cover - the book only exposes positive quantities
            microprice = mid

        mid_ticks = (bid_px + ask_px) / 2.0
        if mid_ticks > 0.0:
            spread_bps = (ask_px - bid_px) / mid_ticks * 1e4
        else:  # pragma: no cover - positive exchange prices are assumed
            spread_bps = 0.0

        # Top-5 depth imbalance, in raw quantity units (the ratio is unitless).
        bids, asks = book.top_levels(_DEPTH_LEVELS)
        bid_depth = sum(qty for _, qty in bids)
        ask_depth = sum(qty for _, qty in asks)
        total_depth = bid_depth + ask_depth
        if total_depth > 0:
            depth_imbalance = (bid_depth - ask_depth) / total_depth
        else:  # pragma: no cover - the book has a visible two-sided quote
            depth_imbalance = 0.0

        # Trade imbalance over the rolling window.
        if state.trade_gross > 0.0:
            trade_imbalance = state.trade_signed / state.trade_gross
        else:
            trade_imbalance = 0.0

        # EWMA volatility of squared log mid returns.
        if state.ewma_var > 0.0:
            realized_vol_bps = math.sqrt(state.ewma_var) * 1e4
        else:
            realized_vol_bps = 0.0

        # Momentum against the oldest point inside the momentum window.  It is
        # suppressed until enough history exists to avoid fabricated signal.
        momentum = 0.0
        if state.mids:
            oldest_ts, oldest_mid = state.mids[0]
            if (
                ts_ms - oldest_ts >= self._momentum_min_history_ms
                and oldest_mid > 0.0
            ):
                momentum = mid / oldest_mid - 1.0

        ofi_1s = state.ofi_sum

        return FactorSnapshot(
            symbol=ev.symbol,
            ts_ms=ts_ms,
            seq=ev.seq,
            mid=_finite_or(mid),
            microprice=_finite_or(microprice),
            spread_bps=_finite_or(spread_bps),
            ofi_1s=_finite_or(ofi_1s),
            depth_imbalance_5=_clamp_unit(_finite_or(depth_imbalance)),
            trade_imbalance_1s=_clamp_unit(_finite_or(trade_imbalance)),
            realized_vol_bps=max(0.0, _finite_or(realized_vol_bps)),
            momentum_5s=_finite_or(momentum),
            best_bid=_finite_or(bid_px / TICKS_PER_UNIT),
            best_ask=_finite_or(ask_px / TICKS_PER_UNIT),
            bid_qty=_finite_or(bid_qty_ticks / TICKS_PER_UNIT),
            ask_qty=_finite_or(ask_qty_ticks / TICKS_PER_UNIT),
        )

    # ------------------------------------------------------------------
    # State and window helpers
    # ------------------------------------------------------------------
    def _state(self, symbol: str) -> _SymbolState:
        """Return (creating on demand) the state for ``symbol``."""
        key = symbol.upper()
        state = self._states.get(key)
        if state is None:
            state = _SymbolState()
            self._states[key] = state
        return state

    def _purge_ofi(self, state: _SymbolState, now_ms: int) -> None:
        """Drop OFI contributions older than the rolling window."""
        cutoff = now_ms - self._ofi_window_ms
        events = state.ofi_events
        while events and events[0][0] < cutoff:
            _, delta = events.popleft()
            state.ofi_sum -= delta
        if not events:
            # Avoid floating-point drift accumulating over a long run.
            state.ofi_sum = 0.0

    def _purge_mids(self, state: _SymbolState, now_ms: int) -> None:
        """Drop mid observations older than the momentum window."""
        cutoff = now_ms - self._momentum_window_ms
        mids = state.mids
        while mids and mids[0][0] < cutoff:
            mids.popleft()

    def _purge_trades(self, state: _SymbolState, now_ms: int) -> None:
        """Drop trades older than the trade-imbalance window."""
        cutoff = now_ms - self._trade_window_ms
        trades = state.trade_events
        while trades and trades[0][0] < cutoff:
            _, signed = trades.popleft()
            state.trade_signed -= signed
            state.trade_gross -= abs(signed)
        if not trades:
            state.trade_signed = 0.0
            state.trade_gross = 0.0


__all__ = ["FactorEngine"]
