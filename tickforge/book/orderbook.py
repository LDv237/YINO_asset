"""Binance spot L2 order book with strict diff-depth synchronisation.

The order book consumes two event types produced by the feed layer:

* :class:`~tickforge.events.SnapshotEvent` -- a REST depth snapshot with a
  ``lastUpdateId``.
* :class:`~tickforge.events.DepthEvent` -- a ``<symbol>@depth`` diff update
  carrying the exchange sequence range ``[U, u]``.

Synchronisation follows the Binance spot rules exactly:

1. The book starts in ``awaiting_snapshot`` state.  Diff events received in
   that state are buffered (bounded by ``max_buffer``).
2. A snapshot rebuilds the book.  Buffered events are then replayed in arrival
   order, discarding anything already covered by the snapshot.  The first
   surviving event must satisfy ``U <= lastUpdateId + 1 <= u``; later events
   may overlap but must still satisfy ``U <= last + 1`` and ``u > last``.
3. While synced, ``u <= last`` is a duplicate and ``U > last + 1`` is a gap.
   A gap invalidates synchronisation and re-buffers the offending event.
4. A crossed book (best bid >= best ask) is never exposed: synchronisation is
   invalidated and a fresh snapshot is requested.
5. ``begin_resync()`` clears all state until a new snapshot is applied.

Prices and quantities are integer ticks (1e-8).  Price levels live in
``dict[price, qty]`` maps; each side additionally has a lazy-deletion heap of
prices so that ``best()`` and ``top_levels(n)`` do not need to sort the whole
book.  Deleted levels are removed from the map immediately and pruned from the
heap lazily, at the top.

All methods are synchronous and free of wall-clock/random dependencies, so a
WAL replay reproduces identical book state.
"""

from __future__ import annotations

import heapq
from collections import deque
from dataclasses import dataclass

from ..events import TICKS_PER_UNIT, DepthEvent, SnapshotEvent

#: Status returned by :meth:`OrderBook.apply_depth` / :meth:`OrderBook.apply_snapshot`.
STATUS_APPLIED = "applied"
STATUS_BUFFERED = "buffered"
STATUS_DUPLICATE = "duplicate"
STATUS_GAP = "gap"
STATUS_CROSSED = "crossed"
STATUS_IGNORED = "ignored"


@dataclass(slots=True, frozen=True)
class BestQuote:
    """Best bid/ask levels, in integer ticks."""

    bid_px: int
    bid_qty: int
    ask_px: int
    ask_qty: int


@dataclass(slots=True, frozen=True)
class BookResult:
    """Outcome of applying one depth or snapshot event.

    ``status`` is one of ``applied``, ``buffered``, ``duplicate``, ``gap``,
    ``crossed`` or ``ignored``.  ``synced``/``needs_snapshot`` describe the
    state *after* the call and ``best_changed`` compares the externally
    visible best quote before and after it (an unsynchronised book exposes no
    quote).  ``buffered`` is the number of diff events currently buffered.
    """

    status: str
    synced: bool
    needs_snapshot: bool
    best_changed: bool
    buffered: int


class OrderBook:
    """A single-symbol Binance spot order book kept in sync with diff depth.

    Args:
        symbol: Exchange symbol, e.g. ``"BTCUSDT"``.  Comparisons are
            case-insensitive.
        max_buffer: Maximum number of diff events retained while the book is
            not synchronised.  On overflow the oldest event is dropped, the
            ``buffer_overflow`` counter is incremented and a new snapshot is
            required.

    The public read accessors (:meth:`best`, :meth:`top_levels`, :meth:`mid`,
    :meth:`microprice`, :meth:`depth_imbalance`) expose nothing while the book
    is not synchronised, so a stale or crossed book can never leak into factor
    or strategy calculations.
    """

    __slots__ = (
        "_symbol",
        "_symbol_upper",
        "_max_buffer",
        "_bids",
        "_asks",
        "_bid_heap",
        "_ask_heap",
        "_bid_queued",
        "_ask_queued",
        "_buffer",
        "_synced",
        "_needs_snapshot",
        "_last_update_id",
        "_applied",
        "_duplicates",
        "_gaps",
        "_snapshots",
        "_crossed",
        "_buffer_overflow",
        "_ignored",
        "_stale_snapshots",
        "_resyncs",
    )

    def __init__(self, symbol: str, *, max_buffer: int = 50_000) -> None:
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be a non-empty string")
        if not isinstance(max_buffer, int) or max_buffer < 1:
            raise ValueError("max_buffer must be an integer >= 1")

        self._symbol = symbol
        self._symbol_upper = symbol.upper()
        self._max_buffer = max_buffer

        # price -> remaining quantity (positive only; zero means absent)
        self._bids: dict[int, int] = {}
        self._asks: dict[int, int] = {}
        # lazy-deletion heaps: bids store negated prices (max-heap)
        self._bid_heap: list[int] = []
        self._ask_heap: list[int] = []
        # prices that currently have exactly one entry in the corresponding heap
        self._bid_queued: set[int] = set()
        self._ask_queued: set[int] = set()

        # diff events received while not synchronised
        self._buffer: deque[DepthEvent] = deque()

        self._synced = False
        self._needs_snapshot = True
        self._last_update_id = 0

        # counters exposed through stats()
        self._applied = 0
        self._duplicates = 0
        self._gaps = 0
        self._snapshots = 0
        self._crossed = 0
        self._buffer_overflow = 0
        # diagnostic counters (not part of the required stats() schema)
        self._ignored = 0
        self._stale_snapshots = 0
        self._resyncs = 0

    # ------------------------------------------------------------------
    # State properties
    # ------------------------------------------------------------------
    @property
    def synced(self) -> bool:
        """True while the book is synchronised with the exchange stream."""
        return self._synced

    @property
    def needs_snapshot(self) -> bool:
        """True when a new REST snapshot is required to resume synchronisation."""
        return self._needs_snapshot

    @property
    def last_update_id(self) -> int:
        """Last applied exchange update id (``0`` before any snapshot)."""
        return self._last_update_id

    # ------------------------------------------------------------------
    # Event application
    # ------------------------------------------------------------------
    def apply_depth(self, ev: DepthEvent) -> BookResult:
        """Apply one Binance diff-depth event.

        While synchronised this implements the exchange sequencing rules; while
        awaiting a snapshot the event is buffered (subject to ``max_buffer``).
        """
        prev = self._visible_best()

        if (
            ev.symbol.upper() != self._symbol_upper
            or ev.first_update_id < 0
            or ev.final_update_id < ev.first_update_id
        ):
            self._ignored += 1
            return self._result(STATUS_IGNORED, prev)

        if not self._synced:
            self._buffer_event(ev)
            return self._result(STATUS_BUFFERED, prev)

        last = self._last_update_id
        if ev.final_update_id <= last:
            self._duplicates += 1
            return self._result(STATUS_DUPLICATE, prev)

        if ev.first_update_id > last + 1:
            # Missing updates: keep this event for the next snapshot and stop
            # treating the (now stale) book as usable.
            self._gaps += 1
            self._synced = False
            self._needs_snapshot = True
            self._buffer_event(ev)
            return self._result(STATUS_GAP, prev)

        self._apply_levels(ev)
        self._last_update_id = ev.final_update_id
        self._applied += 1

        if self._is_crossed():
            self._crossed += 1
            self._synced = False
            self._needs_snapshot = True
            # Keep the event: a snapshot fetched earlier may still overlap it.
            self._buffer_event(ev)
            return self._result(STATUS_CROSSED, prev)

        return self._result(STATUS_APPLIED, prev)

    def apply_snapshot(self, ev: SnapshotEvent) -> BookResult:
        """Rebuild the book from a REST snapshot and replay buffered diffs.

        Returns ``applied`` when the book becomes synchronised, ``buffered``
        when the snapshot is too old for the buffered stream (the buffer is
        kept untouched and the book stays hidden), ``crossed`` if the rebuilt
        book is crossed, or ``ignored`` for a symbol mismatch.
        """
        prev = self._visible_best()

        if ev.symbol.upper() != self._symbol_upper or ev.last_update_id < 0:
            self._ignored += 1
            return self._result(STATUS_IGNORED, prev)

        # A snapshot fetched earlier can be enqueued after newer depth events
        # (REST latency + ingestion lock contention).  Never let it move the
        # book's update id or price ladder backwards; that would expose stale
        # quotes and turn the next contiguous diff into a spurious gap.
        if ev.last_update_id < self._last_update_id:
            self._stale_snapshots += 1
            if not self._synced:
                self._needs_snapshot = True
            return self._result(STATUS_IGNORED, prev)

        self._snapshots += 1
        self._rebuild(ev)
        self._last_update_id = ev.last_update_id
        # Stay unsynchronised until the buffered stream proves the snapshot
        # recent enough (or the buffer is empty).
        self._synced = False
        self._needs_snapshot = True
        return self._replay_buffer(prev)

    def begin_resync(self) -> None:
        """Discard the book and the buffer and wait for a fresh snapshot."""
        self._clear_side(self._bids, self._bid_heap, self._bid_queued)
        self._clear_side(self._asks, self._ask_heap, self._ask_queued)
        self._buffer.clear()
        self._last_update_id = 0
        self._synced = False
        self._needs_snapshot = True
        self._resyncs += 1

    # ------------------------------------------------------------------
    # Read accessors
    # ------------------------------------------------------------------
    def best(self) -> BestQuote | None:
        """Best bid/ask in ticks, or ``None`` when unsynchronised/one-sided."""
        return self._visible_best()

    def top_levels(self, n: int = 5) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
        """Return up to ``n`` best levels per side without sorting the book.

        The result is ``(bids descending by price, asks ascending by price)``;
        zero-quantity levels are skipped.  An unsynchronised book returns two
        empty lists so that stale data is never exposed.
        """
        if n <= 0 or not self._synced:
            return [], []
        bids = self._top_side(n, self._bids, self._bid_heap, self._bid_queued, True)
        asks = self._top_side(n, self._asks, self._ask_heap, self._ask_queued, False)
        return bids, asks

    def mid(self) -> float | None:
        """Mid price in price units (not ticks), or ``None`` without a quote."""
        quote = self._visible_best()
        if quote is None:
            return None
        return (quote.bid_px + quote.ask_px) / (2 * TICKS_PER_UNIT)

    def microprice(self) -> float | None:
        """Quantity-weighted microprice in price units, or ``None``.

        ``(bid_px * ask_qty + ask_px * bid_qty) / (bid_qty + ask_qty)``.
        """
        quote = self._visible_best()
        if quote is None:
            return None
        total_qty = quote.bid_qty + quote.ask_qty
        if total_qty <= 0:
            return None
        weighted = quote.bid_px * quote.ask_qty + quote.ask_px * quote.bid_qty
        return weighted / total_qty / TICKS_PER_UNIT

    def depth_imbalance(self, n: int = 5) -> float:
        """Top-``n`` quantity imbalance in ``[-1, 1]``; ``0.0`` without a book."""
        if n <= 0 or not self._synced:
            return 0.0
        bids = self._top_side(n, self._bids, self._bid_heap, self._bid_queued, True)
        asks = self._top_side(n, self._asks, self._ask_heap, self._ask_queued, False)
        bid_qty = sum(qty for _, qty in bids)
        ask_qty = sum(qty for _, qty in asks)
        total = bid_qty + ask_qty
        if total <= 0:
            return 0.0
        return (bid_qty - ask_qty) / total

    def stats(self) -> dict:
        """Return deterministic counters describing the book state.

        ``bids``/``asks`` are the number of live price levels per side.
        """
        return {
            "symbol": self._symbol,
            "synced": self._synced,
            "last_update_id": self._last_update_id,
            "bids": len(self._bids),
            "asks": len(self._asks),
            "buffered": len(self._buffer),
            "applied": self._applied,
            "duplicates": self._duplicates,
            "gaps": self._gaps,
            "snapshots": self._snapshots,
            "crossed": self._crossed,
            "buffer_overflow": self._buffer_overflow,
            "stale_snapshots": self._stale_snapshots,
        }

    # ------------------------------------------------------------------
    # Internals: buffering and snapshot replay
    # ------------------------------------------------------------------
    def _buffer_event(self, ev: DepthEvent) -> None:
        """Append a diff event to the bounded pending buffer."""
        if len(self._buffer) >= self._max_buffer:
            self._buffer.popleft()
            self._buffer_overflow += 1
            self._needs_snapshot = True
        self._buffer.append(ev)

    def _replay_buffer(self, prev: BestQuote | None) -> BookResult:
        """Replay buffered diff events on top of a freshly rebuilt book."""
        pending = list(self._buffer)
        total = len(pending)
        last = self._last_update_id

        # Events fully covered by the snapshot are stale.  Find the first
        # candidate without mutating anything: a too-old snapshot must leave
        # the buffer untouched.
        first_index = 0
        while first_index < total and pending[first_index].final_update_id <= last:
            first_index += 1

        if first_index == total:
            # The snapshot already includes every buffered event.
            self._duplicates += total
            self._buffer.clear()
            self._synced = True
            self._needs_snapshot = False
            if self._is_crossed():
                self._crossed += 1
                self._synced = False
                self._needs_snapshot = True
                return self._result(STATUS_CROSSED, prev)
            return self._result(STATUS_APPLIED, prev)

        if pending[first_index].first_update_id > last + 1:
            # Snapshot too old for the buffered stream: wait for a newer one,
            # keeping the buffer (including its stale prefix) intact.
            self._synced = False
            self._needs_snapshot = True
            return self._result(STATUS_BUFFERED, prev)

        # From here on the snapshot is usable; discard the stale prefix.
        if first_index:
            self._duplicates += first_index
            self._buffer = deque(pending[first_index:])
        pending = pending[first_index:]
        last = self._last_update_id
        index = 0

        while index < len(pending):
            ev = pending[index]
            if ev.final_update_id <= last:
                self._duplicates += 1
                index += 1
                continue
            if ev.first_update_id > last + 1:
                # Gap inside the buffered stream: keep this event and the
                # remaining ones for the next snapshot.
                self._gaps += 1
                self._buffer = deque(pending[index:])
                self._synced = False
                self._needs_snapshot = True
                return self._result(STATUS_GAP, prev)

            self._apply_levels(ev)
            last = ev.final_update_id
            self._last_update_id = last
            self._applied += 1
            index += 1

            if self._is_crossed():
                self._crossed += 1
                # Keep the crossing event itself: it can still be replayed
                # idempotently if the next snapshot overlaps it.
                self._buffer = deque(pending[index - 1:])
                self._synced = False
                self._needs_snapshot = True
                return self._result(STATUS_CROSSED, prev)

        self._buffer.clear()
        self._synced = True
        self._needs_snapshot = False
        return self._result(STATUS_APPLIED, prev)

    # ------------------------------------------------------------------
    # Internals: book mutation
    # ------------------------------------------------------------------
    def _rebuild(self, ev: SnapshotEvent) -> None:
        """Replace the whole book with the snapshot's levels."""
        self._clear_side(self._bids, self._bid_heap, self._bid_queued)
        self._clear_side(self._asks, self._ask_heap, self._ask_queued)
        for price, qty in ev.bids:
            if price > 0:
                self._set_level(
                    self._bids, self._bid_heap, self._bid_queued, price, qty, True
                )
        for price, qty in ev.asks:
            if price > 0:
                self._set_level(
                    self._asks, self._ask_heap, self._ask_queued, price, qty, False
                )

    @staticmethod
    def _clear_side(book: dict[int, int], heap: list[int], queued: set[int]) -> None:
        book.clear()
        heap.clear()
        queued.clear()

    @staticmethod
    def _set_level(
        book: dict[int, int],
        heap: list[int],
        queued: set[int],
        price: int,
        qty: int,
        negate: bool,
    ) -> None:
        """Apply one absolute level update.

        ``qty <= 0`` deletes the level.  A price is pushed at most once while
        it is live; if it was pruned from the heap top earlier it is pushed
        again when it reappears.
        """
        if qty <= 0:
            book.pop(price, None)
            return
        book[price] = qty
        if price not in queued:
            heapq.heappush(heap, -price if negate else price)
            queued.add(price)

    def _apply_levels(self, ev: DepthEvent) -> None:
        """Apply both sides of a diff event as absolute quantity updates."""
        for price, qty in ev.bids:
            if price > 0:
                self._set_level(
                    self._bids, self._bid_heap, self._bid_queued, price, qty, True
                )
        for price, qty in ev.asks:
            if price > 0:
                self._set_level(
                    self._asks, self._ask_heap, self._ask_queued, price, qty, False
                )
        # Keep memory bounded when prices churn far from the top of the book.
        self._maybe_compact(self._bids, self._bid_heap, self._bid_queued, True)
        self._maybe_compact(self._asks, self._ask_heap, self._ask_queued, False)

    @staticmethod
    def _maybe_compact(
        book: dict[int, int],
        heap: list[int],
        queued: set[int],
        negate: bool,
    ) -> None:
        """Rebuild the heap when lazy-deletion entries dominate.

        Compaction is amortised ``O(L)`` (heapify, not a sort) and runs only
        when the heap has grown well beyond the number of live levels.
        """
        if len(heap) <= 2 * len(book) + 64:
            return
        heap[:] = [-price if negate else price for price in book]
        heapq.heapify(heap)
        queued.clear()
        queued.update(book)

    # ------------------------------------------------------------------
    # Internals: best-price lookup and top-N extraction
    # ------------------------------------------------------------------
    def _best_bid_price(self) -> int | None:
        """Return the highest live bid price, pruning stale heap entries."""
        heap = self._bid_heap
        book = self._bids
        queued = self._bid_queued
        while heap:
            price = -heap[0]
            if price in book:
                return price
            heapq.heappop(heap)
            queued.discard(price)
        return None

    def _best_ask_price(self) -> int | None:
        """Return the lowest live ask price, pruning stale heap entries."""
        heap = self._ask_heap
        book = self._asks
        queued = self._ask_queued
        while heap:
            price = heap[0]
            if price in book:
                return price
            heapq.heappop(heap)
            queued.discard(price)
        return None

    def _visible_best(self) -> BestQuote | None:
        """Best quote exposed to callers; ``None`` unless fully synchronised."""
        if not self._synced:
            return None
        bid_px = self._best_bid_price()
        ask_px = self._best_ask_price()
        if bid_px is None or ask_px is None or bid_px >= ask_px:
            return None
        return BestQuote(
            bid_px=bid_px,
            bid_qty=self._bids[bid_px],
            ask_px=ask_px,
            ask_qty=self._asks[ask_px],
        )

    def _is_crossed(self) -> bool:
        """True when both sides exist and best bid >= best ask."""
        bid_px = self._best_bid_price()
        ask_px = self._best_ask_price()
        return bid_px is not None and ask_px is not None and bid_px >= ask_px

    def _top_side(
        self,
        n: int,
        book: dict[int, int],
        heap: list[int],
        queued: set[int],
        negate: bool,
    ) -> list[tuple[int, int]]:
        """Extract the top ``n`` live levels using the lazy-deletion heap.

        Valid entries are popped and pushed back, so the heap is unchanged
        apart from stale entries that get discarded.  Cost is ``O((n + stale)
        * log L)`` -- never a full sort of the book.
        """
        out: list[tuple[int, int]] = []
        popped: list[int] = []
        while heap and len(out) < n:
            key = heap[0]
            price = -key if negate else key
            qty = book.get(price)
            if qty is None or qty <= 0:
                heapq.heappop(heap)
                queued.discard(price)
                continue
            heapq.heappop(heap)
            popped.append(key)
            out.append((price, qty))
        for key in popped:
            heapq.heappush(heap, key)
        return out

    # ------------------------------------------------------------------
    # Internals: result construction
    # ------------------------------------------------------------------
    def _result(self, status: str, prev: BestQuote | None) -> BookResult:
        """Build a :class:`BookResult` after the state transition."""
        return BookResult(
            status=status,
            synced=self._synced,
            needs_snapshot=self._needs_snapshot,
            best_changed=self._visible_best() != prev,
            buffered=len(self._buffer),
        )


__all__ = [
    "BestQuote",
    "BookResult",
    "OrderBook",
    "STATUS_APPLIED",
    "STATUS_BUFFERED",
    "STATUS_CROSSED",
    "STATUS_DUPLICATE",
    "STATUS_GAP",
    "STATUS_IGNORED",
]
