#!/usr/bin/env python3
"""Run a short live process, kill it without graceful shutdown, then inspect WAL.

This is an application-level crash test, not a power-loss test: already-fsynced
records should remain readable and CRC/seq-valid; a partial tail may be
reported.  No clean summary is expected from the killed process.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tickforge.store.wal import WalReader


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/spot_long_only_durable.toml")
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--warmup", type=float, default=8.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--out", default="artifacts/experiments/crash_recovery")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stdout_path = out / "child_console.log"
    cmd = [
        sys.executable,
        "-m",
        "tickforge",
        "live",
        "--config",
        args.config,
        "--duration",
        "120",
        "--symbols",
        args.symbols,
        "--out",
        str(out),
        "--http-port",
        "0",
    ]
    with stdout_path.open("wb") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        # Wait for a few WAL bytes, then kill without clean shutdown.
        deadline = time.monotonic() + args.warmup
        while time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.2)
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=args.timeout)

    runs = sorted((out / "runs").glob("*"), key=lambda p: p.stat().st_mtime)
    if not runs:
        print(json.dumps({"ok": False, "error": "no run directory produced"}))
        return 1
    run_dir = runs[-1]
    report = {
        "ok": True,
        "run_dir": str(run_dir),
        "child_returncode": proc.returncode,
        "note": (
            "Process was killed without graceful shutdown. This is an "
            "application-level crash test, not a power-loss test; no "
            "clean_shutdown/summary is expected."
        ),
    }
    try:
        scan = WalReader(
            run_dir / "raw", verify_crc=True, allow_truncated_tail=True
        ).scan()
        report["wal"] = {
            "records": scan.records,
            "records_by_kind": {str(k): v for k, v in scan.records_by_kind.items()},
            "bytes": scan.bytes,
            "segments": scan.segments,
            "first_seq": scan.first_seq,
            "last_seq": scan.last_seq,
            "truncated_tail_bytes": scan.truncated_tail_bytes,
            "errors": scan.errors,
        }
        report["ok"] = not any(
            error for error in scan.errors if not error.startswith("truncated tail")
        )
    except Exception as exc:
        report["ok"] = False
        report["wal_error"] = f"{type(exc).__name__}: {exc}"

    (out / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
