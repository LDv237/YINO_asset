#!/usr/bin/env python3
"""Summarize a TickForge run for the performance report.

Usage: python tools/analyze_run.py artifacts/runs/<run_id> [--json]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load(run_dir: Path) -> dict[str, Any]:
    with (run_dir / "summary.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)


def human_ns(value: int | float | None) -> str:
    if value is None:
        return "-"
    value = float(value)
    if value >= 1e9:
        return f"{value / 1e9:.3f} s"
    if value >= 1e6:
        return f"{value / 1e6:.3f} ms"
    if value >= 1e3:
        return f"{value / 1e3:.3f} us"
    return f"{value:.0f} ns"


def print_report(summary: dict[str, Any]) -> None:
    print(f"run_id              {summary.get('run_id')}")
    print(f"mode                {summary.get('mode')}")
    print(f"started_at          {summary.get('started_at')}")
    print(f"measure_end_at      {summary.get('measure_end_at')}")
    print(f"ended_at            {summary.get('ended_at')}")
    print(f"window_s            {summary.get('window_s')}")
    print(f"shutdown_s          {summary.get('shutdown_s')}")
    print(f"clean_shutdown      {summary.get('clean_shutdown')}")
    print(f"end_reason          {summary.get('end_reason')}")
    gaps = summary.get("data_gaps") or []
    print(f"data_gaps           {len(gaps)}")
    for gap in gaps[:5]:
        print(f"  - {gap}")

    counts = summary.get("counts") or {}
    pipe = counts.get("pipeline") or {}
    print("\nCounts")
    for key in (
        "ws_frames_received",
        "ws_bytes_received",
        "processed_envelopes",
        "connections",
        "disconnects",
        "reconnects",
        "snapshots_enqueued",
        "snapshot_errors",
        "wal_backpressure_events",
        "pipeline_backpressure_events",
        "processing_errors",
        "fatal_errors",
    ):
        print(f"  {key:28s} {counts.get(key)}")
    for key in (
        "snapshots",
        "snapshots_synced",
        "depth_events",
        "trade_events",
        "bookticker_events",
        "gaps",
        "crossed",
        "resyncs",
        "factor_snapshots",
        "signals",
        "fills",
        "parse_errors",
        "unknown_events",
    ):
        print(f"  pipeline.{key:19s} {pipe.get(key)}")

    print("\nRates")
    for key, value in (summary.get("rates") or {}).items():
        print(f"  {key:28s} {value}")

    resources = summary.get("resources") or {}
    if resources:
        print("\nResources")
        for key, value in resources.items():
            print(f"  {key:28s} {value}")

    wal = summary.get("wal") or {}
    print("\nWAL")
    for key in (
        "records_ws",
        "records_snapshot",
        "records_total",
        "bytes",
        "segments",
        "fsync_policy",
        "fsyncs",
        "fsync_total_ns",
        "error",
    ):
        print(f"  {key:28s} {wal.get(key)}")

    print("\nLatency (receive -> stage completion)")
    latency = summary.get("latency_ns") or {}
    for name in sorted(latency):
        h = latency[name]
        print(
            f"  {name:42s} n={h.get('count'):>8} "
            f"p50={human_ns(h.get('p50_ns')):>10} "
            f"p90={human_ns(h.get('p90_ns')):>10} "
            f"p99={human_ns(h.get('p99_ns')):>10} "
            f"p999={human_ns(h.get('p999_ns')):>10} "
            f"max={human_ns(h.get('max_ns')):>10}"
        )

    print("\nBooks")
    books = (summary.get("pipeline") or {}).get("books") or {}
    for symbol, stats in sorted(books.items()):
        print(
            f"  {symbol:10s} synced={stats.get('synced')} "
            f"last_update_id={stats.get('last_update_id')} "
            f"bids={stats.get('bids')} asks={stats.get('asks')} "
            f"applied={stats.get('applied')} gaps={stats.get('gaps')} "
            f"snapshots={stats.get('snapshots')} buffered={stats.get('buffered')}"
        )

    sim = summary.get("simulator") or {}
    if sim:
        print("\nSimulator")
        print(json.dumps(sim, ensure_ascii=False, indent=2)[:4000])

    print("\nReconciliation")
    print(json.dumps(summary.get("reconciliation") or {}, ensure_ascii=False, indent=2))
    print("\nDigests")
    print(json.dumps(summary.get("digests") or {}, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    summary = load(run_dir)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print_report(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
