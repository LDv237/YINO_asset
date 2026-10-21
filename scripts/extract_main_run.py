#!/usr/bin/env python3
"""Extract the main run's raw/ and derived/ from run_data.tar.xz."""
from __future__ import annotations

import argparse
import tarfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", help="e.g. 运行材料/20261021T162501Z-9821")
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    archive = run_dir / "run_data.tar.xz"
    if not archive.is_file():
        raise SystemExit(f"archive not found: {archive}")
    with tarfile.open(archive, "r:xz") as tf:
        try:
            tf.extractall(run_dir, filter="data")
        except TypeError:  # Python < 3.12
            tf.extractall(run_dir)
    print(f"extracted {archive} -> {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
