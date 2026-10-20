#!/usr/bin/env bash
# Usage: scripts/replay_verify.sh artifacts/runs/<run_id>
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONUTF8=1
RUN_DIR="${1:?usage: replay_verify.sh <run_dir>}"
python -m tickforge verify --run "$RUN_DIR"
python -m tickforge replay --run "$RUN_DIR" --out "$RUN_DIR-replay" --verify
