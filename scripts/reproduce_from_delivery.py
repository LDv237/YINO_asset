#!/usr/bin/env python3
"""Reconstruct a complete run directory from the classified delivery folder.

The classified delivery folders (`07_最终代码单源复跑/` and `02_正式运行/`)
intentionally keep the large files as archives.  This script rebuilds the
exact layout expected by ``tickforge verify``/``replay``:

    <workdir>/raw/raw-*.wal
    <workdir>/derived/factors|signals|fills.jsonl
    <workdir>/config.json
    <workdir>/summary.json       (from 正式运行摘要.json)
    <workdir>/manifest.json      (from 正式运行清单.json)

Then it runs ``verify`` and ``replay --verify``.  Use the full Release
artifacts zip instead if you want the pre-built run directory.

By default the script rebuilds the main result in ``07_最终代码单源复跑``.
Pass ``--delivery 运行材料/02_正式运行`` to rebuild the historical 53b5
baseline instead.  The workdir is derived from ``run_id`` in 正式运行摘要.json,
so the two runs do not collide.  Existing non-empty workdirs and replay
outputs are refused unless ``--force``.

Usage::

    python scripts/reproduce_from_delivery.py
    python scripts/reproduce_from_delivery.py \
        --delivery 运行材料/02_正式运行 \
        --workdir artifacts/reproduced/20261020T072820Z-53b5
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DELIVERY = ROOT / "运行材料" / "07_最终代码单源复跑"


def extract(tar_path: Path, dest: Path) -> None:
    with tarfile.open(tar_path, "r:gz") as tf:
        try:
            tf.extractall(dest, filter="data")
        except TypeError:  # Python < 3.12
            tf.extractall(dest)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivery", default=str(DEFAULT_DELIVERY))
    parser.add_argument(
        "--workdir",
        default=None,
        help="default: artifacts/reproduced/<run_id from 正式运行摘要.json>",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing non-empty workdir and replay output",
    )
    args = parser.parse_args()

    src = Path(args.delivery)
    if not src.is_dir():
        raise SystemExit(f"delivery folder not found: {src}")
    required = [
        "原始WAL.tar.gz",
        "derived.tar.gz",
        "config.json",
        "正式运行摘要.json",
        "正式运行清单.json",
    ]
    missing = [name for name in required if not (src / name).is_file()]
    if missing:
        raise SystemExit(
            "missing files in classified delivery: "
            + ", ".join(missing)
            + "\nUse the full Release artifacts zip, or add derived.tar.gz "
            "(the classified copy alone is not a complete run directory)."
        )

    try:
        summary = json.loads((src / "正式运行摘要.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"cannot read 正式运行摘要.json: {exc}")
    run_id = str(summary.get("run_id") or "").strip()
    if args.workdir:
        work = Path(args.workdir)
    elif run_id:
        work = ROOT / "artifacts" / "reproduced" / run_id
    else:
        raise SystemExit("cannot determine run_id from 正式运行摘要.json; pass --workdir")
    print(f"run_id: {run_id or '<custom>'}", flush=True)
    print(f"workdir: {work}", flush=True)
    if work.exists() and not work.is_dir():
        raise SystemExit(f"workdir exists and is not a directory: {work}")
    if work.is_dir() and any(work.iterdir()):
        if not args.force:
            raise SystemExit(f"workdir is not empty: {work} (use --force)")
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    print(f"[1/4] extracting WAL -> {work / 'raw'}", flush=True)
    extract(src / "原始WAL.tar.gz", work)
    print(f"[2/4] extracting derived -> {work / 'derived'}", flush=True)
    extract(src / "derived.tar.gz", work)
    print("[3/4] copying config/summary/manifest", flush=True)
    shutil.copy2(src / "config.json", work / "config.json")
    shutil.copy2(src / "正式运行摘要.json", work / "summary.json")
    shutil.copy2(src / "正式运行清单.json", work / "manifest.json")

    replay_out = work.parent / f"{work.name}-replay"
    if replay_out.exists():
        if not replay_out.is_dir():
            raise SystemExit(f"replay output exists and is not a directory: {replay_out}")
        if any(replay_out.iterdir()):
            if not args.force:
                raise SystemExit(f"replay output is not empty: {replay_out} (use --force)")
            shutil.rmtree(replay_out)
        else:
            replay_out.rmdir()
    commands = [
        [sys.executable, "-m", "tickforge", "verify", "--run", str(work)],
        [
            sys.executable,
            "-m",
            "tickforge",
            "replay",
            "--run",
            str(work),
            "--out",
            str(replay_out),
            "--verify",
        ],
    ]
    print(f"[4/4] verifying reconstructed run: {work}", flush=True)
    for cmd in commands:
        print("+", " ".join(cmd), flush=True)
        result = subprocess.run(cmd, cwd=ROOT)
        if result.returncode != 0:
            return result.returncode
    print(f"\nOK: reconstructed run at {work}")
    print(f"replay output at {replay_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
