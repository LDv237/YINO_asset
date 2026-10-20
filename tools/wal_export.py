#!/usr/bin/env python3
"""Export a TickForge binary WAL to human-readable JSONL.

The WAL remains the canonical, checksummed source of truth.  This tool is a
read-only view for reviewers: one JSON object per line, preserving the exact
sequence number, receive timestamps and the original Binance payload.

Usage::

    python tools/wal_export.py --run artifacts/runs/<run_id> \
        --out artifacts/raw_events.jsonl.gz
    python tools/wal_export.py --run samples/demo_run \
        --out samples/demo_run/raw_events.jsonl.gz --max-records 20000
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import orjson

from tickforge.store.wal import KIND_SNAPSHOT, KIND_WS, WalReader


def _decode_payload(payload: bytes):
    try:
        return orjson.loads(payload)
    except orjson.JSONDecodeError:
        # Keep even a malformed raw frame visible and auditable.
        return {"payload_base64": base64.b64encode(payload).decode("ascii")}


def export(run_dir: Path, out_path: Path, max_records: int | None = None) -> dict:
    raw_dir = run_dir / "raw"
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"raw WAL directory not found: {raw_dir}")
    reader = WalReader(raw_dir, verify_crc=True, allow_truncated_tail=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gz = out_path.suffix.lower() == ".gz"
    opener = gzip.open if gz else open
    records = 0
    ws = 0
    snapshots = 0
    with opener(out_path, "wt", encoding="utf-8", compresslevel=6) as fh:
        for record in reader:
            if max_records is not None and records >= max_records:
                break
            payload = _decode_payload(record.payload)
            if record.kind == KIND_WS:
                ws += 1
                stream = payload.get("stream", "") if isinstance(payload, dict) else ""
                data = payload.get("data", payload) if isinstance(payload, dict) else payload
                obj = {
                    "seq": record.seq,
                    "kind": "ws",
                    "stream": stream,
                    "recv_wall_ns": record.recv_wall_ns,
                    "recv_mono_ns": record.recv_mono_ns,
                    "data": data,
                }
            elif record.kind == KIND_SNAPSHOT:
                snapshots += 1
                obj = {
                    "seq": record.seq,
                    "kind": "snapshot",
                    "recv_wall_ns": record.recv_wall_ns,
                    "recv_mono_ns": record.recv_mono_ns,
                    "data": payload,
                }
            else:  # pragma: no cover - defensive
                obj = {
                    "seq": record.seq,
                    "kind": record.kind,
                    "recv_wall_ns": record.recv_wall_ns,
                    "recv_mono_ns": record.recv_mono_ns,
                    "data": payload,
                }
            fh.write(orjson.dumps(obj).decode("utf-8") + "\n")
            records += 1
    return {
        "run_dir": str(run_dir),
        "out": str(out_path),
        "records": records,
        "ws_records": ws,
        "snapshot_records": snapshots,
        "bytes": out_path.stat().st_size,
        "truncated_by_max_records": max_records is not None and records >= max_records,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="stop after N records (useful for a small sample)",
    )
    args = parser.parse_args()
    result = export(Path(args.run), Path(args.out), args.max_records)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
