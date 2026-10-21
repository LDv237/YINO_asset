"""Live runtime: WebSocket ingress, write-ahead log, pipeline, health, shutdown.

Ownership of ordering is deliberate:

    socket frame -> timestamp -> WAL queue -> processing queue -> pipeline

The raw frame is enqueued for durable storage *before* the processor sees it.
A bounded WAL queue and a bounded processing queue apply backpressure to the
socket reader instead of dropping data.  The only optional stronger guarantee
is ``wal.fsync_policy="always"``, which waits for fsync before processing and
is measured separately because it trades latency for crash durability.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import platform
import secrets
import socket
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psutil

from tickforge.config import Config
from tickforge.feed.binance import BinanceFeed
from tickforge.feed.rest import SnapshotClient
from tickforge.metrics import MetricsRegistry
from tickforge.normalize import encode_snapshot
from tickforge.runtime.pipeline import Envelope, Pipeline
from tickforge.server.http import HttpServer
from tickforge.store.derived import JsonlSink
from tickforge.store.wal import (
    KIND_SNAPSHOT,
    KIND_WS,
    WalClosed,
    WalError,
    WalManifest,
    WalQueueFull,
    WalWriter,
)

log = logging.getLogger(__name__)

RUN_METADATA = "config.json"
SUMMARY_FILE = "summary.json"
MANIFEST_FILE = "manifest.json"
CHECKSUM_FILE = "sha256sums.txt"
STARTUP_TIMEOUT_S = 120.0


def utc_iso(ns: int | None = None) -> str:
    if ns is None:
        ns = time.time_ns()
    seconds = ns / 1e9
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + f".{ns % 1_000_000_000:09d}Z"


def new_run_id(now_ns: int | None = None) -> str:
    now_ns = time.time_ns() if now_ns is None else now_ns
    base = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ns / 1e9))
    return f"{base}-{secrets.token_hex(2)}"


def prepare_run_dir(base: str | Path, run_id: str | None = None) -> tuple[Path, str]:
    run_id = run_id or new_run_id()
    safe = "".join(ch for ch in run_id if ch.isalnum() or ch in "-._")
    if not safe:
        raise ValueError("run_id must contain alphanumeric characters")
    run_dir = Path(base) / "runs" / safe
    if run_dir.exists() and any(run_dir.iterdir()):
        raise FileExistsError(f"run directory already exists and is not empty: {run_dir}")
    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "derived").mkdir(parents=True, exist_ok=True)
    return run_dir, safe


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=False, default=str)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


@dataclass(slots=True)
class LiveCounts:
    ws_frames: int = 0
    ws_bytes: int = 0
    wal_backpressure: int = 0
    pipeline_backpressure: int = 0
    connections: int = 0
    disconnects: int = 0
    reconnects: int = 0
    snapshot_errors: int = 0
    snapshots_enqueued: int = 0
    processing_errors: int = 0
    processed_envelopes: int = 0
    not_enqueued_on_shutdown: int = 0
    fatal_errors: int = 0
    wal_queue_max: int = 0
    pipeline_queue_max: int = 0


class LiveEngine:
    def __init__(
        self,
        cfg: Config,
        run_dir: str | Path,
        *,
        run_id: str | None = None,
        logger: logging.Logger | None = None,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self.cfg = cfg
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        for sub in ("raw", "logs", "derived"):
            (self.run_dir / sub).mkdir(parents=True, exist_ok=True)
        self.run_id = run_id or self.run_dir.name
        self.log = logger or log
        self.metrics = metrics or MetricsRegistry()

        self._stop_event = asyncio.Event()
        self._processor_stop = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping = False
        self._stop_reason = "stopped"
        self._started_wall_ns = time.time_ns()
        self._measure_end_wall_ns = 0
        self._first_event_wall_ns = 0
        self._health_at_stop: dict[str, Any] | None = None
        self._last_ingress_mono_ns = 0
        self._last_processed_recv_mono_ns = 0
        self._last_processed_mono_ns = 0
        self._disconnect_started_wall_ns = 0
        self._data_gaps: list[dict[str, Any]] = []
        self._seq = 0
        self.live = LiveCounts()
        self.queue: asyncio.Queue[Envelope] = asyncio.Queue(
            maxsize=cfg.engine.proc_queue_max
        )
        self._ingest_lock = asyncio.Lock()
        self._snapshot_tasks: dict[str, asyncio.Task] = {}
        self._snapshot_last_enqueue_ns: dict[str, int] = {}
        self._feed_task: asyncio.Task | None = None
        self._proc_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._http_started = False
        self._wal_started = False
        self._shutdown_done = False

        self.factor_sink = JsonlSink(self.run_dir / "derived" / "factors.jsonl")
        self.signal_sink = JsonlSink(self.run_dir / "derived" / "signals.jsonl")
        self.fill_sink = JsonlSink(self.run_dir / "derived" / "fills.jsonl")

        self.wal = WalWriter(
            self.run_dir / "raw",
            segment_bytes=cfg.wal.segment_bytes,
            fsync_interval_ms=cfg.wal.fsync_interval_ms,
            fsync_policy=cfg.wal.fsync_policy,
            queue_max=cfg.wal.queue_max,
            record_max_bytes=cfg.wal.record_max_bytes,
            logger=self.log,
            on_durable_lag_ns=self._observe_durable_lag,
        )
        self.pipeline = Pipeline(
            cfg,
            self.metrics,
            factor_sink=self.factor_sink,
            signal_sink=self.signal_sink,
            fill_sink=self.fill_sink,
            request_snapshot=self._schedule_snapshot,
            observe_latency=True,
            logger=self.log,
        )
        self.rest = SnapshotClient(cfg.feed, logger=self.log, metrics=self.metrics)
        self.feed = BinanceFeed(
            cfg.feed,
            on_message=self._on_message,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
            logger=self.log,
            metrics=self.metrics,
        )
        self.http = HttpServer(
            self.metrics,
            health_fn=self.health_snapshot,
            status_fn=self.status_snapshot,
            host=cfg.http.host,
            port=cfg.http.port,
            logger=self.log,
        )
        self._psutil = psutil.Process()
        try:
            self._psutil.cpu_percent(None)  # prime the counter
        except Exception:
            pass
        self._last_rss_bytes = 0
        self._peak_rss_bytes = 0
        self._last_cpu_percent = 0.0
        self._cpu_sum = 0.0
        self._cpu_samples = 0
        self._peak_ws_rate = 0.0
        self._rate_last_ws = 0
        self._rate_last_mono = time.monotonic()

    # --------------------------------------------------------------- lifecycle
    async def run(self) -> dict:
        self._loop = asyncio.get_running_loop()
        self._write_metadata()
        self.log.info(
            "run starting",
            extra={
                "run_id": self.run_id,
                "symbols": ",".join(self.cfg.feed.symbols),
                "ws_url": self.cfg.feed.ws_url,
                "rest_url": self.cfg.feed.rest_url,
                "duration_s": self.cfg.run.duration_s,
                "fsync_policy": self.cfg.wal.fsync_policy,
                "run_dir": str(self.run_dir),
            },
        )
        try:
            self.wal.start()
            self._wal_started = True
            if self.cfg.http.enabled:
                await self.http.start()
                self._http_started = True
            self._proc_task = asyncio.create_task(
                self._processor_loop(), name="pipeline-processor"
            )
            self._watchdog_task = asyncio.create_task(
                self._watchdog_loop(), name="watchdog"
            )
            self._feed_task = asyncio.create_task(self.feed.run(), name="binance-feed")
            self._feed_task.add_done_callback(self._on_feed_done)
            await self._stop_event.wait()
        except asyncio.CancelledError:
            self.request_stop("cancelled")
        finally:
            summary = await self._shutdown()
        return summary

    def request_stop(self, reason: str = "requested") -> None:
        if self._stopping:
            return
        self._stop_reason = reason
        if self._measure_end_wall_ns == 0:
            self._measure_end_wall_ns = time.time_ns()
        self.log.info("stop requested", extra={"reason": reason})
        try:
            if self._loop is not None and self._loop.is_running():
                self._loop.call_soon_threadsafe(self._stop_event.set)
            else:
                self._stop_event.set()
        except RuntimeError:
            self._stop_event.set()

    def _on_feed_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.live.fatal_errors += 1
            self.log.error("feed task exited with exception: %s", exc, exc_info=exc)
        elif not self._stopping:
            self.log.error("feed task exited unexpectedly")
        if not self._stopping:
            self.request_stop("feed_task_exited")

    # --------------------------------------------------------------- ingestion
    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def _on_message(
        self, payload: bytes, recv_wall_ns: int, recv_mono_ns: int
    ) -> None:
        self.live.ws_frames += 1
        self.live.ws_bytes += len(payload)
        if self._first_event_wall_ns == 0:
            self._first_event_wall_ns = recv_wall_ns
            self.metrics.set_gauge("tickforge_time_to_first_event_ms", (recv_wall_ns - self._started_wall_ns) / 1e6)
            self.log.info(
                "first market data received",
                extra={"latency_ms": (recv_wall_ns - self._started_wall_ns) / 1e6},
            )
        self._last_ingress_mono_ns = recv_mono_ns

        try:
            await self._ingest(KIND_WS, payload, recv_wall_ns, recv_mono_ns)
        except (WalClosed, WalError) as exc:
            self.live.fatal_errors += 1
            self.log.error("WAL failure while ingesting: %s", exc)
            self.request_stop("wal_failure")
            return
        self.metrics.observe_ns(
            "tickforge_latency_ingest_enqueue_ns",
            max(0, time.perf_counter_ns() - recv_mono_ns),
        )
        # WebSocket recv() may return many already-buffered frames in a burst.
        # Yield periodically so the processor is not starved by a hot read
        # loop; this materially improves the e2e tail without batching costs.
        if self.live.ws_frames % 16 == 0:
            await asyncio.sleep(0)

    async def _ingest(
        self, kind: int, payload: bytes, recv_wall_ns: int, recv_mono_ns: int
    ) -> int:
        """Persist one input event and enqueue it, preserving global order.

        A single lock serializes the (rare) snapshot producer with the socket
        producer.  The sequence number is assigned immediately before the WAL
        append, so sequence order, WAL order and processing order are one
        invariant.  Persisted events are never dropped during shutdown; the
        enqueue wait has a generous deadline so a fatal processor hang cannot
        wedge the process forever.
        """

        async with self._ingest_lock:
            deadline = time.monotonic() + max(30.0, self.cfg.engine.drain_timeout_s)
            while True:
                while self.wal.queue_depth >= max(1, self.cfg.wal.queue_max - 1):
                    wal_stats = self.wal.stats()
                    if wal_stats.get("error"):
                        raise WalClosed(f"WAL writer failed: {wal_stats['error']}")
                    if time.monotonic() > deadline:
                        self.live.not_enqueued_on_shutdown += 1
                        raise WalClosed("deadline waiting for WAL queue")
                    self.live.wal_backpressure += 1
                    self.metrics.inc("tickforge_wal_backpressure_total")
                    await asyncio.sleep(0.001)
                seq = self._next_seq()
                try:
                    if kind == KIND_WS:
                        self.wal.append_ws(
                            payload,
                            recv_wall_ns=recv_wall_ns,
                            recv_mono_ns=recv_mono_ns,
                            seq=seq,
                        )
                    else:
                        self.wal.append_snapshot(
                            payload,
                            recv_wall_ns=recv_wall_ns,
                            recv_mono_ns=recv_mono_ns,
                            seq=seq,
                        )
                    self.live.wal_queue_max = max(
                        self.live.wal_queue_max, self.wal.queue_depth
                    )
                    break
                except WalQueueFull:
                    self.live.wal_backpressure += 1
                    self.metrics.inc("tickforge_wal_backpressure_total")
                    await asyncio.sleep(0.001)
                except WalClosed:
                    raise
                except Exception as exc:
                    self.live.fatal_errors += 1
                    self.log.error("WAL append failed: %s", exc, exc_info=exc)
                    raise WalError(str(exc)) from exc

            self.metrics.inc(
                "tickforge_wal_records_total",
                {"kind": "ws" if kind == KIND_WS else "snapshot"},
            )
            self.metrics.inc("tickforge_wal_bytes_total", value=len(payload))

            if self.cfg.wal.fsync_policy == "always":
                try:
                    await asyncio.to_thread(self.wal.flush_and_sync)
                except Exception as exc:
                    self.live.fatal_errors += 1
                    self.log.error("WAL fsync failed: %s", exc)
                    raise WalError(str(exc)) from exc

            env = Envelope(
                kind=kind,
                seq=seq,
                recv_wall_ns=recv_wall_ns,
                recv_mono_ns=recv_mono_ns,
                payload=payload,
            )
            while True:
                try:
                    self.queue.put_nowait(env)
                    self.metrics.set_gauge(
                        "tickforge_pipeline_queue_depth", self.queue.qsize()
                    )
                    self.live.pipeline_queue_max = max(
                        self.live.pipeline_queue_max, self.queue.qsize()
                    )
                    return seq
                except asyncio.QueueFull:
                    if time.monotonic() > deadline:
                        self.live.not_enqueued_on_shutdown += 1
                        self.log.error(
                            "deadline waiting for processing queue; event only in WAL",
                            extra={"seq": seq},
                        )
                        return seq
                    self.live.pipeline_backpressure += 1
                    self.metrics.inc("tickforge_pipeline_backpressure_total")
                    await asyncio.sleep(0.001)

    # ---------------------------------------------------------------- callbacks
    async def _on_connect(self, conn_id: int, is_reconnect: bool) -> None:
        self.live.connections += 1
        if is_reconnect:
            self.live.reconnects += 1
            if self._disconnect_started_wall_ns:
                end = time.time_ns()
                start = self._disconnect_started_wall_ns
                self._data_gaps.append(
                    {
                        "start": utc_iso(start),
                        "end": utc_iso(end),
                        "duration_ms": round((end - start) / 1e6, 3),
                        "reason": "websocket_reconnect",
                    }
                )
                self._disconnect_started_wall_ns = 0
            # Do not blindly reset every book here: that would be an
            # unlogged state transition replay cannot reproduce.  Binance's
            # per-symbol update-id continuity check detects any missed depth
            # event on the next update and triggers a snapshot through the
            # normal pipeline.  If nothing was missed, the book is still
            # valid and continues seamlessly.
            self.log.warning(
                "websocket reconnected; sequence continuity will decide resync",
                extra={"conn_id": conn_id},
            )
        else:
            self.log.info("websocket connected", extra={"conn_id": conn_id})

    async def _on_disconnect(self, conn_id: int, reason: str) -> None:
        self.live.disconnects += 1
        if self._disconnect_started_wall_ns == 0 and conn_id > 0:
            self._disconnect_started_wall_ns = time.time_ns()
        self.log.warning(
            "websocket disconnected",
            extra={"conn_id": conn_id, "reason": reason},
        )

    # --------------------------------------------------------------- snapshots
    def _schedule_snapshot(self, symbol: str, *, force: bool = False) -> None:
        if self._stopping:
            return
        task = self._snapshot_tasks.get(symbol)
        if task is not None and not task.done():
            return
        now = time.perf_counter_ns()
        last = self._snapshot_last_enqueue_ns.get(symbol, 0)
        if not force and now - last < 2_000_000_000:
            return
        try:
            task = asyncio.create_task(
                self._snapshot_worker(symbol), name=f"snapshot-{symbol}"
            )
        except RuntimeError:
            return
        self._snapshot_tasks[symbol] = task

        def _done(t: asyncio.Task, sym: str = symbol) -> None:
            if self._snapshot_tasks.get(sym) is t:
                self._snapshot_tasks.pop(sym, None)

        task.add_done_callback(_done)

    async def _snapshot_worker(self, symbol: str) -> None:
        book = self.pipeline.books[symbol]
        backoff = max(0.1, self.cfg.feed.snapshot_retry_backoff_s)
        while not self._stopping and not book.synced:
            try:
                data = await self.rest.fetch(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.live.snapshot_errors += 1
                self.log.warning(
                    "snapshot fetch failed",
                    extra={"symbol": symbol, "error": str(exc)},
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue

            if book.synced:  # another snapshot won the race
                return
            payload = encode_snapshot(
                symbol=data.symbol,
                last_update_id=data.last_update_id,
                bids=data.bids,
                asks=data.asks,
                requested_mono_ns=data.requested_mono_ns,
                recv_mono_ns=data.recv_mono_ns,
                recv_wall_ns=data.recv_wall_ns,
            )
            try:
                await self._ingest(
                    KIND_SNAPSHOT,
                    payload,
                    data.recv_wall_ns,
                    data.recv_mono_ns,
                )
            except (WalClosed, WalError):
                return
            self._snapshot_last_enqueue_ns[symbol] = time.perf_counter_ns()
            self.live.snapshots_enqueued += 1
            return

    # --------------------------------------------------------------- processor
    async def _processor_loop(self) -> None:
        while True:
            if self._processor_stop.is_set() and self.queue.empty():
                return
            try:
                env = await asyncio.wait_for(self.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if self._processor_stop.is_set() and self.queue.empty():
                    return
                continue
            self._process_one(env)
            # Drain a small batch without awaiting; this amortizes loop wakeups
            # during bursts while still yielding between batches.
            for _ in range(63):
                try:
                    env = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._process_one(env)

    def _process_one(self, env: Envelope) -> None:
        try:
            self.pipeline.process(env)
        except Exception:
            self.live.processing_errors += 1
            self.metrics.inc("tickforge_pipeline_internal_errors_total")
            self.log.exception(
                "pipeline error", extra={"seq": env.seq, "kind": env.kind}
            )
            if self.live.processing_errors >= 100:
                self.request_stop("too_many_pipeline_errors")
        finally:
            self.queue.task_done()
            self.live.processed_envelopes += 1
            self._last_processed_mono_ns = time.perf_counter_ns()
            self._last_processed_recv_mono_ns = env.recv_mono_ns
            self.metrics.set_gauge(
                "tickforge_pipeline_queue_depth", self.queue.qsize()
            )

    # ----------------------------------------------------------------- watchdog
    async def _watchdog_loop(self) -> None:
        interval = max(1.0, self.cfg.engine.stats_interval_s)
        next_stats = time.monotonic() + interval
        next_resource = time.monotonic()
        last_ws = 0
        last_processed = 0
        while not self._stopping:
            now_wall = time.time_ns()
            now_mono = time.perf_counter_ns()
            if (
                self.cfg.run.duration_s > 0
                and self._first_event_wall_ns > 0
                and now_wall - self._first_event_wall_ns
                >= self.cfg.run.duration_s * 1e9
            ):
                self.request_stop("duration_elapsed")
                break
            if (
                self._first_event_wall_ns == 0
                and self.cfg.run.duration_s > 0
                and now_wall - self._started_wall_ns > STARTUP_TIMEOUT_S * 1e9
            ):
                self.log.error(
                    "no market data received within startup timeout; stopping"
                )
                self.request_stop("startup_timeout")
                break

            monotonic = time.monotonic()
            if monotonic >= next_resource:
                self._update_resource_gauges()
                self.metrics.set_gauge("tickforge_wal_queue_depth", self.wal.queue_depth)
                self.metrics.set_gauge(
                    "tickforge_pipeline_processing_lag_ms", self._processing_lag_ms()
                )
                # A book can stay unsynced without new depth events (for
                # example after a failed snapshot on a quiet symbol).  Retry
                # from the health loop; the in-flight task check prevents
                # duplicate requests, and force bypasses the 2s throttle.
                for symbol, book in self.pipeline.books.items():
                    if not book.synced:
                        self._schedule_snapshot(symbol, force=True)
                next_resource += max(0.2, self.cfg.engine.health_interval_s)

            if monotonic >= next_stats:
                window = max(1e-9, monotonic - (next_stats - interval))
                delta_ws = self.live.ws_frames - last_ws
                delta_proc = self.live.processed_envelopes - last_processed
                self.metrics.set_gauge(
                    "tickforge_ingest_rate_per_s", delta_ws / window
                )
                self.metrics.set_gauge(
                    "tickforge_process_rate_per_s", delta_proc / window
                )
                last_ws = self.live.ws_frames
                last_processed = self.live.processed_envelopes
                self._log_stats(delta_ws / window, delta_proc / window)
                next_stats += interval

            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                return

    def _update_resource_gauges(self) -> None:
        try:
            rss = int(self._psutil.memory_info().rss)
            cpu = float(self._psutil.cpu_percent(None))
            self._last_rss_bytes = rss
            self._peak_rss_bytes = max(self._peak_rss_bytes, rss)
            self._last_cpu_percent = cpu
            self._cpu_sum += cpu
            self._cpu_samples += 1
            self.metrics.set_gauge("tickforge_process_rss_bytes", rss)
            self.metrics.set_gauge("tickforge_process_cpu_percent", cpu)
            now = time.monotonic()
            elapsed = now - self._rate_last_mono
            if elapsed > 0:
                rate = (self.live.ws_frames - self._rate_last_ws) / elapsed
                self._peak_ws_rate = max(self._peak_ws_rate, rate)
            self._rate_last_ws = self.live.ws_frames
            self._rate_last_mono = now
        except Exception:
            pass

    def _processing_lag_ms(self) -> float:
        if self.queue.empty() or self._last_ingress_mono_ns <= 0:
            return 0.0
        lag = self._last_ingress_mono_ns - self._last_processed_recv_mono_ns
        return round(max(0, lag) / 1e6, 3)

    def _log_stats(self, ws_rate: float, proc_rate: float) -> None:
        pipe = self.pipeline.counts
        self.log.info(
            "runtime stats",
            extra={
                "ws_msgs": self.live.ws_frames,
                "ws_msg_per_s": round(ws_rate, 1),
                "processed": self.live.processed_envelopes,
                "proc_per_s": round(proc_rate, 1),
                "queue": self.queue.qsize(),
                "wal_queue": self.wal.queue_depth,
                "depth": pipe["depth_events"],
                "trades": pipe["trade_events"],
                "factors": pipe["factor_snapshots"],
                "signals": pipe["signals"],
                "fills": pipe["fills"],
                "gaps": pipe["gaps"],
                "resyncs": pipe["resyncs"],
                "errors": pipe["parse_errors"] + self.live.processing_errors,
            },
        )

    # ------------------------------------------------------------------ health
    def health_snapshot(self) -> dict:
        now_mono = time.perf_counter_ns()
        now_wall = time.time_ns()
        reasons: list[str] = []
        connected = bool(getattr(self.feed, "connected", False))
        last_msg = int(getattr(self.feed, "last_msg_mono_ns", 0) or 0)
        # During an intentional shutdown the feed is supposed to be closed;
        # that is not an operational failure.  Unexpected disconnects while
        # running still fail health.
        if not connected and not self._stopping:
            reasons.append("feed_disconnected")
        if (
            last_msg
            and not self._stopping
            and now_mono - last_msg > self.cfg.feed.stale_timeout_ms * 1e6
        ):
            reasons.append("feed_stale")
        if self._first_event_wall_ns == 0:
            reasons.append("waiting_for_data")
        elif now_wall - self._first_event_wall_ns > 5 * 1e9:
            unsynced = [
                symbol
                for symbol, book in self.pipeline.books.items()
                if not book.synced
            ]
            if unsynced:
                reasons.append("books_unsynced:" + ",".join(sorted(unsynced)))
        wal_stats = self.wal.stats()
        if wal_stats.get("error"):
            reasons.append("wal_error")
        lag = self._processing_lag_ms()
        if lag > self.cfg.engine.max_processing_lag_ms:
            reasons.append("processing_lag")
        if self.queue.maxsize and self.queue.qsize() >= self.queue.maxsize * 0.9:
            reasons.append("pipeline_queue_near_full")
        if self.live.processing_errors >= 50:
            reasons.append("pipeline_errors")
        return {
            "ok": not reasons,
            "reasons": reasons,
            "run_id": self.run_id,
            "uptime_s": round((now_wall - self._started_wall_ns) / 1e9, 3),
            "feed": {
                "connected": connected,
                "last_message_age_ms": (
                    round((now_mono - last_msg) / 1e6, 3) if last_msg else None
                ),
                "messages": self.live.ws_frames,
                "connections": self.live.connections,
                "reconnects": self.live.reconnects,
            },
            "books": {
                symbol: {
                    "synced": book.synced,
                    "last_update_id": book.last_update_id,
                    "gaps": book.stats().get("gaps", 0),
                    "buffered": book.stats().get("buffered", 0),
                }
                for symbol, book in self.pipeline.books.items()
            },
            "wal": {
                "queue_depth": self.wal.queue_depth,
                "error": wal_stats.get("error"),
                "records": wal_stats.get("records_ws", 0)
                + wal_stats.get("records_snapshot", 0),
            },
            "pipeline": {
                "queue_depth": self.queue.qsize(),
                "processing_lag_ms": lag,
                "processed": self.live.processed_envelopes,
            },
        }

    def status_snapshot(self) -> dict:
        health = self.health_snapshot()
        wal_stats = self.wal.stats()
        return {
            "run_id": self.run_id,
            "mode": "live",
            "started_at": utc_iso(self._started_wall_ns),
            "health": health,
            "source": {
                "ws_url": self.cfg.feed.ws_url,
                "rest_url": self.cfg.feed.rest_url,
                "symbols": self.cfg.feed.symbols,
                "streams": self.cfg.feed.streams(),
            },
            "counts": {
                "live": asdict(self.live),
                "pipeline": dict(self.pipeline.counts),
            },
            "feed": {
                "connected": bool(getattr(self.feed, "connected", False)),
                "connections": getattr(self.feed, "connections", 0),
                "messages": getattr(self.feed, "messages", 0),
            },
            "wal": wal_stats,
            "pipeline": self.pipeline.stats(),
            "metrics": self.metrics.snapshot(),
            "data_gaps": self._data_gaps,
        }

    # ----------------------------------------------------------------- shutdown
    async def _shutdown(self) -> dict:
        if self._shutdown_done:
            return {}
        # Capture readiness before we intentionally close the feed.
        self._health_at_stop = self.health_snapshot()
        self._stopping = True
        self._stop_event.set()
        self.log.info("shutdown begin", extra={"reason": self._stop_reason})
        end_reason = self._stop_reason

        try:
            if self._feed_task is not None:
                await self.feed.stop()
                try:
                    await asyncio.wait_for(self._feed_task, timeout=10)
                except asyncio.TimeoutError:
                    self._feed_task.cancel()
                    await asyncio.gather(self._feed_task, return_exceptions=True)
        except Exception as exc:
            self.log.warning("error stopping feed: %s", exc)

        for task in list(self._snapshot_tasks.values()):
            task.cancel()
        if self._snapshot_tasks:
            await asyncio.gather(
                *list(self._snapshot_tasks.values()), return_exceptions=True
            )
            self._snapshot_tasks.clear()

        # Drain the processing queue; the raw frames are already durable.
        deadline = time.monotonic() + self.cfg.engine.drain_timeout_s
        while not self.queue.empty() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        drained = self.queue.empty()
        if not drained:
            self.log.error(
                "processing queue not drained before shutdown",
                extra={"remaining": self.queue.qsize()},
            )

        self._processor_stop.set()
        if self._proc_task is not None:
            try:
                await asyncio.wait_for(self._proc_task, timeout=5)
            except asyncio.TimeoutError:
                self._proc_task.cancel()
                await asyncio.gather(self._proc_task, return_exceptions=True)
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            await asyncio.gather(self._watchdog_task, return_exceptions=True)

        digests: dict[str, str] = {}
        sink_info: dict[str, Any] = {}
        sink_errors: dict[str, str] = {}
        for name, sink in (
            ("factors", self.factor_sink),
            ("signals", self.signal_sink),
            ("fills", self.fill_sink),
        ):
            try:
                result = sink.close()
                digests[f"{name}_sha256"] = result.sha256
                sink_info[name] = asdict(result)
            except Exception as exc:
                # A derived file that could not be flushed, closed and hashed
                # is incomplete output, not a cosmetic warning: fail the run.
                sink_errors[name] = f"{type(exc).__name__}: {exc}"
                self.log.error(
                    "failed closing %s sink: %s", name, exc, exc_info=exc
                )

        wal_manifest: WalManifest | None = None
        wal_error: str | None = None
        if self._wal_started:
            try:
                wal_manifest = await asyncio.to_thread(self.wal.close, 30.0)
            except Exception as exc:
                wal_error = str(exc)
                self.log.error("failed closing WAL: %s", exc, exc_info=exc)

        try:
            await self.rest.close()
        except Exception:
            pass

        summary = self._build_summary(
            wal_manifest=wal_manifest,
            digests=digests,
            sink_info=sink_info,
            sink_errors=sink_errors,
            drained=drained,
            wal_error=wal_error,
            end_reason=end_reason,
        )
        write_json(self.run_dir / SUMMARY_FILE, summary)
        manifest = self._build_manifest(summary, wal_manifest)
        write_json(self.run_dir / MANIFEST_FILE, manifest)
        self._write_checksums()
        if self._http_started:
            try:
                await self.http.stop()
            except Exception:
                pass
        self._shutdown_done = True
        self.log.info(
            "run complete",
            extra={
                "run_dir": str(self.run_dir),
                "ws_msgs": self.live.ws_frames,
                "processed": self.live.processed_envelopes,
                "wal_records": (wal_manifest.records if wal_manifest else None),
                "signals": self.pipeline.counts["signals"],
                "fills": self.pipeline.counts["fills"],
                "clean": summary["clean_shutdown"],
            },
        )
        return summary

    # ---------------------------------------------------------------- reporting
    def _write_metadata(self) -> None:
        write_json(
            self.run_dir / RUN_METADATA,
            {
                "run_id": self.run_id,
                "mode": "live",
                "created_at": utc_iso(self._started_wall_ns),
                "config": self.cfg.to_dict(),
                "environment": {
                    "python": sys.version,
                    "platform": platform.platform(),
                    "cpu_count": os.cpu_count(),
                    "hostname": socket.gethostname(),
                    "pid": os.getpid(),
                },
            },
        )

    def _window_seconds(self) -> float:
        if self._first_event_wall_ns <= 0:
            return 0.0
        end = time.time_ns()
        return max(0.0, (end - self._first_event_wall_ns) / 1e9)

    def _observe_durable_lag(self, lag_ns: int) -> None:
        self.metrics.observe_ns("tickforge_latency_wal_durable_ns", lag_ns)

    def _build_summary(
        self,
        *,
        wal_manifest: WalManifest | None,
        digests: dict[str, str],
        sink_info: dict[str, Any],
        sink_errors: dict[str, str],
        drained: bool,
        wal_error: str | None,
        end_reason: str,
    ) -> dict:
        now = time.time_ns()
        measure_end = self._measure_end_wall_ns or now
        window = (
            (measure_end - self._first_event_wall_ns) / 1e9
            if self._first_event_wall_ns > 0
            else 0.0
        )
        pipe = self.pipeline.counts
        wal_stats = self.wal.stats()
        # Manifest segment counts include both kinds; use writer stats for kind
        # splits, then fall back to the manifest total.
        wal_records_ws = int(wal_stats.get("records_ws", 0))
        wal_records_snapshot = int(wal_stats.get("records_snapshot", 0))
        wal_records_total = int(
            wal_manifest.records if wal_manifest is not None else wal_stats.get("written_seq", 0) or 0
        )
        latency = {
            key: value
            for key, value in self.metrics.snapshot()["histograms"].items()
            if key.startswith("tickforge_latency_")
        }
        rates = {
            "window_s": round(window, 3),
            "ws_msgs_per_s": round(self.live.ws_frames / window, 3) if window else 0,
            "ws_bytes_per_s": round(self.live.ws_bytes / window, 1) if window else 0,
            "peak_ws_msgs_per_s_1s": round(self._peak_ws_rate, 3),
            "depth_events_per_s": round(pipe["depth_events"] / window, 3) if window else 0,
            "factor_snapshots_per_s": round(pipe["factor_snapshots"] / window, 3) if window else 0,
            "signals_per_s": round(pipe["signals"] / window, 3) if window else 0,
        }
        resources = {
            "rss_bytes_last": self._last_rss_bytes,
            "rss_bytes_peak": self._peak_rss_bytes,
            "cpu_percent_last": round(self._last_cpu_percent, 3),
            "cpu_percent_mean": (
                round(self._cpu_sum / self._cpu_samples, 3) if self._cpu_samples else 0.0
            ),
            "cpu_samples": self._cpu_samples,
        }
        lost_raw = max(0, self.live.ws_frames - wal_records_ws)
        unprocessed_persisted = max(
            0,
            (wal_records_ws + wal_records_snapshot) - self.live.processed_envelopes,
        )
        derived_complete = not sink_errors and all(
            f"{name}_sha256" in digests for name in ("factors", "signals", "fills")
        )
        clean = bool(
            drained
            and derived_complete
            and wal_error is None
            and wal_stats.get("error") is None
            and wal_manifest is not None
            and wal_manifest.durable_seq is not None
            and wal_manifest.durable_seq == wal_manifest.last_seq
            and self.live.ws_frames > 0
            and self.live.fatal_errors == 0
            and self.live.processing_errors == 0
            and lost_raw == 0
            and unprocessed_persisted == 0
            and self.live.not_enqueued_on_shutdown == 0
        )
        summary = {
            "run_id": self.run_id,
            "mode": "live",
            "started_at": utc_iso(self._started_wall_ns),
            "first_event_at": (
                utc_iso(self._first_event_wall_ns) if self._first_event_wall_ns else None
            ),
            "measure_end_at": utc_iso(measure_end),
            "ended_at": utc_iso(now),
            "duration_s": round((measure_end - self._started_wall_ns) / 1e9, 3),
            "shutdown_s": round((now - measure_end) / 1e9, 3),
            "window_s": round(window, 3),
            "end_reason": end_reason,
            "clean_shutdown": clean,
            "source": {
                "ws_url": self.cfg.feed.ws_url,
                "rest_url": self.cfg.feed.rest_url,
                "symbols": self.cfg.feed.symbols,
                "streams": self.cfg.feed.streams(),
            },
            "config": self.cfg.to_dict(),
            "environment": {
                "python": sys.version,
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "hostname": socket.gethostname(),
            },
            "counts": {
                "ws_frames_received": self.live.ws_frames,
                "ws_bytes_received": self.live.ws_bytes,
                "processed_envelopes": self.live.processed_envelopes,
                "connections": self.live.connections,
                "disconnects": self.live.disconnects,
                "reconnects": self.live.reconnects,
                "snapshots_enqueued": self.live.snapshots_enqueued,
                "snapshot_errors": self.live.snapshot_errors,
                "wal_backpressure_events": self.live.wal_backpressure,
                "pipeline_backpressure_events": self.live.pipeline_backpressure,
                "processing_errors": self.live.processing_errors,
                "fatal_errors": self.live.fatal_errors,
                "wal_queue_max": self.live.wal_queue_max,
                "pipeline_queue_max": self.live.pipeline_queue_max,
                "pipeline": dict(pipe),
            },
            "rates": rates,
            "resources": resources,
            "wal": {
                "records_ws": wal_records_ws,
                "records_snapshot": wal_records_snapshot,
                "records_total": wal_records_total,
                "bytes": int(wal_manifest.bytes) if wal_manifest else int(wal_stats.get("bytes", 0)),
                "segments": len(wal_manifest.segments) if wal_manifest else 0,
                "fsync_policy": self.cfg.wal.fsync_policy,
                "fsyncs": int(wal_stats.get("fsyncs", 0)),
                "fsync_total_ns": int(wal_stats.get("fsync_total_ns", 0)),
                "error": wal_error or wal_stats.get("error"),
                "manifest": asdict(wal_manifest) if wal_manifest else None,
            },
            "latency_ns": latency,
            "pipeline": self.pipeline.stats(),
            "simulator": self.pipeline.simulator.summary(),
            "digests": digests,
            "derived": sink_info,
            "derived_complete": derived_complete,
            "derived_errors": sink_errors,
            "reconciliation": {
                "ws_frames_received": self.live.ws_frames,
                "wal_records_ws": wal_records_ws,
                "ws_frames_processed": pipe["ws_frames"],
                "snapshots_processed": pipe["snapshots"],
                "lost_raw_frames": lost_raw,
                "unprocessed_persisted": unprocessed_persisted,
                "unprocessed_at_shutdown": self.queue.qsize(),
                "not_enqueued_at_shutdown": self.live.not_enqueued_on_shutdown,
                "drained": drained,
            },
            "data_gaps": self._data_gaps,
            "health_before_shutdown": self._health_at_stop,
            "health_final": self.health_snapshot(),
            "health_final_context": "after_intentional_shutdown",
        }
        return summary

    def _build_manifest(
        self, summary: dict, wal_manifest: WalManifest | None
    ) -> dict:
        files: list[dict[str, Any]] = []
        for path in sorted(self.run_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = str(path.relative_to(self.run_dir)).replace("\\", "/")
            if rel in (MANIFEST_FILE, CHECKSUM_FILE) or rel.startswith("logs/"):
                continue
            try:
                files.append(
                    {
                        "path": rel,
                        "bytes": path.stat().st_size,
                        "sha256": sha256_file(path),
                    }
                )
            except OSError:
                files.append({"path": rel, "bytes": None, "sha256": None})
        logs = []
        log_dir = self.run_dir / "logs"
        if log_dir.exists():
            for path in sorted(log_dir.rglob("*")):
                if path.is_file():
                    logs.append(
                        {
                            "path": str(path.relative_to(self.run_dir)).replace("\\", "/"),
                            "bytes": path.stat().st_size,
                        }
                    )
        return {
            "run_id": self.run_id,
            "created_at": utc_iso(),
            "mode": "live",
            "source": summary["source"],
            "wal": asdict(wal_manifest) if wal_manifest else None,
            "files": files,
            "logs": logs,
            "digests": summary["digests"],
            "counts": summary["counts"],
            "reconciliation": summary["reconciliation"],
            "clean_shutdown": summary["clean_shutdown"],
        }

    def _write_checksums(self) -> None:
        lines = []
        for path in sorted(self.run_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = str(path.relative_to(self.run_dir)).replace("\\", "/")
            if rel in (CHECKSUM_FILE, MANIFEST_FILE) or rel.startswith("logs/"):
                continue
            try:
                lines.append(f"{sha256_file(path)}  {rel}")
            except OSError:
                continue
        (self.run_dir / CHECKSUM_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")


__all__ = [
    "LiveEngine",
    "LiveCounts",
    "Envelope",
    "new_run_id",
    "prepare_run_dir",
    "utc_iso",
    "write_json",
    "sha256_file",
]
