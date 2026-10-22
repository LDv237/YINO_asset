#!/usr/bin/env python3
"""Synthetic pipeline capacity probe.

This tool replays the recorded envelopes of a run through the same
``Pipeline`` used by live/replay, but paces them at a multiple of the original
average ingress rate and inserts a bounded queue in front.  It answers a
narrow question: can the processing stage keep up with N times the observed
load without a growing queue or silent loss?

It is deliberately component-level.  It does not exercise the WebSocket feed,
the WAL writer thread, network RTT or derived-file I/O; those are separate
stages and are reported separately in the submission.  Do not present this as
an end-to-end live capacity proof.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any

# Allow ``python tools/capacity_probe.py`` from a source checkout without
# requiring the package to be installed first.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tickforge.metrics import MetricsRegistry
from tickforge.replay.runner import config_from_run
from tickforge.runtime.pipeline import Envelope, Pipeline
from tickforge.store.wal import WalReader


def _quantile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * p))))
    return ordered[index]


def _read_summary(run_dir: Path) -> dict[str, Any]:
    return json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))


def run_probe(
    run_dir: Path,
    multiplier: float,
    *,
    max_records: int | None = None,
) -> dict[str, Any]:
    raw_dir = run_dir / "raw"
    if not raw_dir.is_dir() or not any(raw_dir.glob("raw-*.wal")):
        raise FileNotFoundError(
            f"raw WAL not found in {raw_dir}; run "
            f"python scripts/extract_main_run.py {run_dir} first"
        )

    cfg = config_from_run(run_dir)
    summary = _read_summary(run_dir)
    base_rate = float(
        (summary.get("rates") or {}).get("ws_msgs_per_s")
        or (
            (summary.get("counts", {}).get("pipeline", {}).get("ws_frames", 0))
            / max(float(summary.get("window_s") or 1.0), 1e-9)
        )
    )
    target_rate = base_rate * multiplier

    metrics = MetricsRegistry()
    pipeline = Pipeline(
        cfg,
        metrics,
        request_snapshot=lambda _symbol: None,
        observe_latency=True,
    )
    max_queue = max(1, int(cfg.engine.proc_queue_max))
    work: queue.Queue[Envelope | None] = queue.Queue(maxsize=max_queue)

    durations_ms: list[float] = []
    errors: list[str] = []
    processed = 0
    enqueued = 0
    full_waits = 0
    queue_peak = 0
    stop_sampler = threading.Event()
    started = time.perf_counter()

    def producer() -> None:
        nonlocal enqueued, full_waits, queue_peak
        try:
            reader = WalReader(
                run_dir / "raw", record_max_bytes=cfg.wal.record_max_bytes
            )
            for index, record in enumerate(reader):
                if max_records is not None and index >= max_records:
                    break
                due = started + index / target_rate
                now = time.perf_counter()
                if due > now:
                    time.sleep(due - now)
                envelope = Envelope(
                    kind=record.kind,
                    seq=record.seq,
                    recv_wall_ns=time.time_ns(),
                    recv_mono_ns=time.perf_counter_ns(),
                    payload=record.payload,
                )
                while True:
                    try:
                        work.put(envelope, timeout=0.05)
                        enqueued += 1
                        queue_peak = max(queue_peak, work.qsize())
                        break
                    except queue.Full:
                        full_waits += 1
                        if stop_sampler.is_set():
                            return
        finally:
            work.put(None)

    def consumer() -> None:
        nonlocal processed
        while True:
            envelope = work.get()
            if envelope is None:
                work.task_done()
                break
            started_ns = time.perf_counter_ns()
            try:
                pipeline.process(envelope)
            except Exception as exc:  # noqa: BLE001 - report, do not hide
                errors.append(f"{type(exc).__name__}: {exc}")
            durations_ms.append((time.perf_counter_ns() - started_ns) / 1e6)
            processed += 1
            work.task_done()

    def sampler() -> None:
        nonlocal queue_peak
        while not stop_sampler.wait(0.05):
            queue_peak = max(queue_peak, work.qsize())

    producer_thread = threading.Thread(target=producer, name="probe-producer")
    consumer_thread = threading.Thread(target=consumer, name="probe-consumer")
    sampler_thread = threading.Thread(target=sampler, name="probe-sampler", daemon=True)
    producer_thread.start()
    consumer_thread.start()
    sampler_thread.start()
    producer_thread.join()
    consumer_thread.join()
    stop_sampler.set()
    sampler_thread.join(timeout=1.0)
    elapsed_s = time.perf_counter() - started

    pipeline_counts = dict(pipeline.counts)
    expected = summary.get("counts", {}).get("pipeline") or {}
    full_stream = max_records is None
    expected_match = None
    if full_stream and expected:
        expected_match = (
            pipeline_counts.get("signals") == expected.get("signals")
            and pipeline_counts.get("fills") == expected.get("fills")
        )

    return {
        "run_id": summary.get("run_id", run_dir.name),
        "multiplier": multiplier,
        "target_per_s": round(target_rate, 3),
        "max_records": max_records,
        "records_seen": enqueued,
        "records_processed": processed,
        "elapsed_s": round(elapsed_s, 3),
        "offered_per_s": round(enqueued / elapsed_s, 3) if elapsed_s else None,
        "processed_per_s": round(processed / elapsed_s, 3) if elapsed_s else None,
        "queue_maxsize": max_queue,
        "queue_peak": queue_peak,
        "queue_full_waits": full_waits,
        "processing_p50_ms": _quantile(durations_ms, 0.50),
        "processing_p99_ms": _quantile(durations_ms, 0.99),
        "processing_p999_ms": _quantile(durations_ms, 0.999),
        "pipeline_counts": pipeline_counts,
        "expected_counts": {
            "signals": expected.get("signals"),
            "fills": expected.get("fills"),
        },
        "full_stream": full_stream,
        "counts_match_live": expected_match,
        "errors": errors[:20],
        "error_count": len(errors),
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, help="recorded run directory")
    parser.add_argument("--rates", default="2,5", help="multipliers, e.g. 2,5")
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--out", default="运行材料/04_容量探针")
    args = parser.parse_args()

    run_dir = Path(args.run)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    rates = [float(part.strip()) for part in args.rates.split(",") if part.strip()]

    results = []
    for multiplier in rates:
        print(f"probing {multiplier:g}x ...", flush=True)
        try:
            result = run_probe(
                run_dir, multiplier, max_records=args.max_records
            )
        except FileNotFoundError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        results.append(result)
        path = out_dir / f"summary_{multiplier:g}x.json"
        path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(
            f"  {multiplier:g}x: target={result['target_per_s']}/s "
            f"processed={result['records_processed']} "
            f"queue_peak={result['queue_peak']}/{result['queue_maxsize']} "
            f"full_waits={result['queue_full_waits']} "
            f"p99={result['processing_p99_ms']}ms",
            flush=True,
        )

    combined = {
        "run_id": results[0]["run_id"] if results else None,
        "note": (
            "component-level pipeline probe; excludes WebSocket feed, WAL writer "
            "and derived-file I/O; not an end-to-end live capacity proof"
        ),
        "results": results,
    }
    (out_dir / "summary.json").write_text(
        json.dumps(combined, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {out_dir / 'summary.json'}")
    return 0 if all(not item["errors"] for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
