#!/usr/bin/env bash
# Compare durability/latency trade-offs.  Each run is 30 seconds, BTCUSDT only.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONUTF8=1
for policy in interval always never; do
  echo "=== fsync_policy=$policy ==="
  python -m tickforge live \
    --config config/default.toml \
    --duration 30 \
    --symbols BTCUSDT \
    --fsync-policy "$policy" \
    --out "artifacts/fsync/$policy" \
    --http-port 0
done
