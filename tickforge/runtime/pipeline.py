"""Synchronous processing pipeline shared by live and replay modes.

Keeping this code path common is the core of the replay/determinism story:
live mode feeds it envelopes received from the socket, replay mode feeds the
exact envelopes read back from the WAL, and both execute identical logic.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable

from tickforge.book import OrderBook
from tickforge.config import Config
from tickforge.events import BookTickerEvent, DepthEvent, TradeEvent
from tickforge.exec.simulator import Simulator
from tickforge.factor import FactorEngine
from tickforge.metrics import MetricsRegistry
from tickforge.normalize import ParseError, decode_snapshot, parse_ws_message
from tickforge.store.wal import KIND_SNAPSHOT, KIND_WS
from tickforge.strategy.mm import MarketMaker
from tickforge.runtime.trade_quality import TradeQualityTracker

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class Envelope:
    """One persisted input event, in WAL order."""

    kind: int
    seq: int
    recv_wall_ns: int
    recv_mono_ns: int
    payload: bytes


_COUNTER_KEYS = (
    "envelopes",
    "ws_frames",
    "snapshots",
    "snapshots_synced",
    "depth_events",
    "trade_events",
    "bookticker_events",
    "unknown_events",
    "parse_errors",
    "applied",
    "buffered",
    "duplicates",
    "gaps",
    "crossed",
    "snapshot_requests",
    "resyncs",
    "factor_snapshots",
    "signals",
    "fills",
    "trade_duplicates",
    "trade_backwards",
    "trade_gaps",
    "trade_missing_ids",
    "internal_errors",
)


class Pipeline:
    def __init__(
        self,
        cfg: Config,
        metrics: MetricsRegistry,
        *,
        factor_sink=None,
        signal_sink=None,
        fill_sink=None,
        request_snapshot: Callable[[str], None] | None = None,
        observe_latency: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self.cfg = cfg
        self.metrics = metrics
        self.factor_sink = factor_sink
        self.signal_sink = signal_sink
        self.fill_sink = fill_sink
        self.request_snapshot = request_snapshot
        self.observe_latency = observe_latency
        self.log = logger or log

        self.books: dict[str, OrderBook] = {
            symbol: OrderBook(symbol, max_buffer=cfg.engine.book_buffer_max)
            for symbol in cfg.feed.symbols
        }
        self.factors = FactorEngine(interval_ms=cfg.engine.factor_interval_ms)
        self.strategy = MarketMaker(cfg.strategy, logger=self.log)
        self.simulator = Simulator(cfg.strategy, symbols=cfg.feed.symbols, logger=self.log)
        self.trade_quality = TradeQualityTracker()
        for symbol in cfg.feed.symbols:
            self.trade_quality.ensure(symbol)
        self.counts: dict[str, int] = {key: 0 for key in _COUNTER_KEYS}
        self._started = time.time_ns()

    # ------------------------------------------------------------------ utils
    def _inc(
        self,
        name: str,
        labels: dict[str, str] | None = None,
        value: float = 1.0,
    ) -> None:
        self.metrics.inc(name, labels, value)

    def _observe_ns(self, name: str, start_ns: int | None) -> None:
        if self.observe_latency and start_ns is not None:
            self.metrics.observe_ns(name, time.perf_counter_ns() - start_ns)

    def _write(self, sink, obj: dict) -> None:
        if sink is not None:
            sink.write(obj)

    def _request(self, symbol: str) -> None:
        self.counts["snapshot_requests"] += 1
        self._inc("tickforge_book_resync_requests_total", {"symbol": symbol})
        if self.request_snapshot is not None:
            self.request_snapshot(symbol)

    # ------------------------------------------------------------- entrypoint
    def process(self, env: Envelope) -> None:
        self.counts["envelopes"] += 1
        self._inc("tickforge_pipeline_events_total")
        if env.kind == KIND_SNAPSHOT:
            self._process_snapshot(env)
        elif env.kind == KIND_WS:
            self._process_ws(env)
        else:
            self.counts["unknown_events"] += 1

    # --------------------------------------------------------------- snapshots
    def _process_snapshot(self, env: Envelope) -> None:
        self.counts["snapshots"] += 1
        try:
            snapshot = decode_snapshot(
                env.payload,
                seq=env.seq,
                recv_mono_ns=env.recv_mono_ns,
                recv_wall_ns=env.recv_wall_ns,
            )
        except ParseError as exc:
            self.counts["parse_errors"] += 1
            self._inc("tickforge_pipeline_parse_errors_total")
            self.log.warning("snapshot parse error seq=%s: %s", env.seq, exc)
            return

        book = self.books.get(snapshot.symbol)
        if book is None:
            self.counts["unknown_events"] += 1
            return

        result = book.apply_snapshot(snapshot)
        self._inc("tickforge_book_snapshots_total", {"symbol": snapshot.symbol})
        if result.synced:
            self.counts["snapshots_synced"] += 1
        self.metrics.set_gauge(
            "tickforge_book_buffered", result.buffered, {"symbol": snapshot.symbol}
        )
        if result.needs_snapshot:
            # Snapshot too old or sequence continuity broke while replaying the
            # buffer; live mode will fetch a fresher one.
            self._request(snapshot.symbol)
        elif result.synced:
            self.log.info(
                "book synced symbol=%s last_update_id=%s buffered=%s",
                snapshot.symbol,
                book.last_update_id,
                result.buffered,
            )

    # ------------------------------------------------------------------- ws
    def _process_ws(self, env: Envelope) -> None:
        self.counts["ws_frames"] += 1
        parse_start = time.perf_counter_ns() if self.observe_latency else None
        try:
            event = parse_ws_message(
                env.payload,
                seq=env.seq,
                recv_mono_ns=env.recv_mono_ns,
                recv_wall_ns=env.recv_wall_ns,
            )
        except ParseError as exc:
            self.counts["parse_errors"] += 1
            self._inc("tickforge_pipeline_parse_errors_total")
            if self.counts["parse_errors"] <= 5:
                self.log.warning("parse error seq=%s: %s", env.seq, exc)
            return
        parse_end = time.perf_counter_ns() if self.observe_latency else None
        self._observe_ns("tickforge_latency_parse_ns", parse_start)

        if event is None:
            self.counts["unknown_events"] += 1
            self._observe_e2e(env)
            return
        if isinstance(event, BookTickerEvent):
            # The full-depth book is authoritative; bookTicker is retained in
            # the WAL as an independent freshness/reference stream.
            self.counts["bookticker_events"] += 1
            self._observe_e2e(env)
            return
        if isinstance(event, TradeEvent):
            self._process_trade(event)
            self._observe_e2e(env)
            return
        if isinstance(event, DepthEvent):
            self._process_depth(event, parse_end)
            self._observe_e2e(env)
            return
        self.counts["unknown_events"] += 1
        self._observe_e2e(env)

    def _process_trade(self, event: TradeEvent) -> None:
        self.counts["trade_events"] += 1
        self._inc("tickforge_trades_total", {"symbol": event.symbol})
        status, missing_ids = self.trade_quality.observe(event)
        self.metrics.set_gauge(
            "tickforge_last_agg_trade_id",
            event.agg_trade_id,
            {"symbol": event.symbol},
        )
        if status in ("duplicate", "backwards"):
            key = "trade_duplicates" if status == "duplicate" else "trade_backwards"
            self.counts[key] += 1
            self._inc(
                "tickforge_trade_duplicates_total",
                {"symbol": event.symbol, "kind": status},
            )
            return
        if status == "gap":
            self.counts["trade_gaps"] += 1
            self.counts["trade_missing_ids"] += missing_ids
            self._inc("tickforge_trade_gaps_total", {"symbol": event.symbol})
            self._inc(
                "tickforge_trade_missing_ids_total",
                {"symbol": event.symbol},
                value=float(missing_ids),
            )
            # The event itself is still processed; we never discard a real
            # trade just because a gap was detected.
        self.factors.on_trade(event)
        fills = self.simulator.on_trade(event)
        for fill in fills:
            self.counts["fills"] += 1
            self._inc("tickforge_fills_total", {"symbol": fill.symbol})
            self._write(self.fill_sink, fill.to_dict())

    def _process_depth(self, event: DepthEvent, _parse_end: int | None) -> None:
        book = self.books.get(event.symbol)
        if book is None:
            self.counts["unknown_events"] += 1
            return
        self.counts["depth_events"] += 1

        was_synced = book.synced
        book_start = time.perf_counter_ns() if self.observe_latency else None
        result = book.apply_depth(event)
        self._observe_ns("tickforge_latency_book_ns", book_start)
        self._inc("tickforge_book_updates_total", {"symbol": event.symbol})
        if result.status == "applied":
            self.counts["applied"] += 1
        elif result.status == "buffered":
            self.counts["buffered"] += 1
        elif result.status == "duplicate":
            self.counts["duplicates"] += 1
        elif result.status == "gap":
            self.counts["gaps"] += 1
        elif result.status == "crossed":
            self.counts["crossed"] += 1
        self.metrics.set_gauge(
            "tickforge_book_buffered", result.buffered, {"symbol": event.symbol}
        )

        if result.status == "duplicate":
            self._inc("tickforge_book_duplicates_total", {"symbol": event.symbol})
        if result.status == "gap":
            self._inc("tickforge_book_gaps_total", {"symbol": event.symbol})
        if result.status == "crossed":
            self._inc("tickforge_book_crossed_total", {"symbol": event.symbol})

        if result.needs_snapshot:
            # The book is not safe to use.  Cancel any resting quote and make
            # sure the factor engine does not compare against a stale best.
            if was_synced:
                self.counts["resyncs"] += 1
                self._inc("tickforge_book_resyncs_total", {"symbol": event.symbol})
                self.log.warning(
                    "book out of sync symbol=%s status=%s last_update_id=%s",
                    event.symbol,
                    result.status,
                    book.last_update_id,
                )
            self.factors.reset_book_state(event.symbol)
            self.simulator.cancel(event.symbol)
            self._request(event.symbol)
            return

        # Duplicates carry no new book state; feeding them to the factor clock
        # would emit snapshots/signals for an unchanged book.
        if result.status != "applied" or not result.synced:
            return

        factor_start = time.perf_counter_ns() if self.observe_latency else None
        snapshot = self.factors.on_depth(book, event)
        self._observe_ns("tickforge_latency_factor_ns", factor_start)
        if snapshot is None:
            return

        self.counts["factor_snapshots"] += 1
        self._inc("tickforge_factor_snapshots_total", {"symbol": event.symbol})
        self._write(self.factor_sink, snapshot.to_dict())

        signal_start = time.perf_counter_ns() if self.observe_latency else None
        signal = self.strategy.on_snapshot(
            snapshot, position=self.simulator.position(event.symbol)
        )
        self._observe_ns("tickforge_latency_signal_ns", signal_start)
        if signal is not None:
            self.counts["signals"] += 1
            self._inc("tickforge_signals_total", {"symbol": event.symbol})
            self._write(self.signal_sink, signal.to_dict())
            self.simulator.apply_signal(signal)
        self.simulator.mark(event.symbol, snapshot.mid)

    def _observe_e2e(self, env: Envelope) -> None:
        if self.observe_latency:
            now = time.perf_counter_ns()
            if env.recv_mono_ns > 0 and now >= env.recv_mono_ns:
                delta = now - env.recv_mono_ns
                # Aggregate series is what the report cites; the per-symbol
                # copy preserves attribution for /metrics without making the
                # headline percentile depend on label aggregation.
                self.metrics.observe_ns("tickforge_latency_e2e_ns", delta)
                if env.kind == KIND_WS:
                    self.metrics.observe_ns(
                        "tickforge_latency_e2e_by_symbol_ns",
                        delta,
                        {"symbol": _symbol_from_frame(env.payload)},
                    )

    # ---------------------------------------------------------------- reporting
    def stats(self) -> dict:
        factor_stats = self.factors.stats() if hasattr(self.factors, "stats") else {}
        strategy_stats = self.strategy.stats() if hasattr(self.strategy, "stats") else {}
        return {
            "counts": dict(self.counts),
            "books": {symbol: book.stats() for symbol, book in self.books.items()},
            "factors": factor_stats,
            "strategy": strategy_stats,
            "simulator": self.simulator.summary(),
            "trade_quality": self.trade_quality.stats(),
            "uptime_s": (time.time_ns() - self._started) / 1e9,
        }


def _symbol_from_frame(payload: bytes) -> str:
    """Cheap best-effort label for latency metrics; never raises.

    The latency histogram is deliberately *not* used for correctness, so a
    failed extraction simply yields the ``unknown`` label.
    """

    try:
        # The symbol is within the first few hundred bytes in practice.  Avoid
        # a full parse on the hot path for a metrics label.
        head = payload[:512]
        marker = b'"s":"'
        idx = head.find(marker)
        if idx < 0:
            return "unknown"
        start = idx + len(marker)
        end = head.find(b'"', start)
        return head[start:end].decode("ascii", "replace") if end > start else "unknown"
    except Exception:
        return "unknown"
