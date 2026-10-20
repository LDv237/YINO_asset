#!/usr/bin/env python3
"""Create a small, self-contained replayable sample from a live run's WAL.

The output directory has the same layout as a run (``raw/`` + ``config.json``)
so it can be replayed with::

    python -m tickforge replay --run samples/demo_run --no-derived

The sample keeps the exact record order/bytes and stops after N records.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

# Allow `python tools/trim_run.py` from the project root: Python puts the
# script's directory (tools/) on sys.path, not the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tickforge.store.wal import KIND_SNAPSHOT, KIND_WS, WalReader, WalWriter


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--records", type=int, default=20_000)
    args = parser.parse_args()

    src = Path(args.run)
    out = Path(args.out)
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"output directory not empty: {out}")
    (out / "raw").mkdir(parents=True, exist_ok=True)

    cfg = src / "config.json"
    if cfg.is_file():
        shutil.copy2(cfg, out / "config.json")

    reader = WalReader(src / "raw", verify_crc=True, allow_truncated_tail=False)
    writer = WalWriter(
        out / "raw",
        segment_bytes=64 * 1024 * 1024,
        fsync_policy="never",
        fsync_interval_ms=1000,
        queue_max=100_000,
    )
    writer.start()
    kept = 0
    ws = 0
    snapshots = 0
    first_wall = None
    last_wall = 0
    started = time.perf_counter()
    for record in reader:
        if kept >= args.records:
            break
        if record.kind == KIND_WS:
            writer.append_ws(
                record.payload,
                recv_wall_ns=record.recv_wall_ns,
                recv_mono_ns=record.recv_mono_ns,
                seq=record.seq,
            )
            ws += 1
        elif record.kind == KIND_SNAPSHOT:
            writer.append_snapshot(
                record.payload,
                recv_wall_ns=record.recv_wall_ns,
                recv_mono_ns=record.recv_mono_ns,
                seq=record.seq,
            )
            snapshots += 1
        else:
            continue
        if first_wall is None:
            first_wall = record.recv_wall_ns
        last_wall = record.recv_wall_ns
        kept += 1
    manifest = writer.close()
    elapsed = time.perf_counter() - started

    meta = {
        "source_run": str(src),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "records": kept,
        "ws_records": ws,
        "snapshot_records": snapshots,
        "source_window_s": round((last_wall - first_wall) / 1e9, 3) if first_wall else 0,
        "copy_elapsed_s": round(elapsed, 3),
        "first_seq": manifest.first_seq,
        "last_seq": manifest.last_seq,
        "wal_bytes": manifest.bytes,
        "segments": manifest.segments,
    }
    (out / "sample_manifest.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8"
    )
    (out / "README_SAMPLE.md").write_text(
        "# TickForge replay sample\n\n"
        f"Truncated from `{src}`: the first {kept} WAL records "
        f"({meta['source_window_s']} s of the original run).\n\n"
        "Replay it offline (no exchange access required):\n\n"
        "```bash\n"
        f"python -m tickforge replay --run {out} --no-derived\n"
        "```\n",
        encoding="utf-8",
    )
    print(json.dumps(meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
