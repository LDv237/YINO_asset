#!/usr/bin/env bash
# 15-minute live measurement run. Extra CLI flags can be appended.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONUTF8=1
exec python -m tickforge live \
  --config config/default.toml \
  --duration 900 \
  --out artifacts \
  "$@"
