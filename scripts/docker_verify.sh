#!/usr/bin/env bash
# Build the image and produce the P0 evidence: build, offline replay, live 60s,
# WAL verify and deterministic replay inside the container.
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v docker >/dev/null 2>&1; then
  DOCKER="docker"
elif [ -x "/c/Program Files/Docker/Docker/resources/bin/docker.exe" ]; then
  DOCKER="/c/Program Files/Docker/Docker/resources/bin/docker.exe"
else
  echo "ERROR: docker is not installed or not on PATH" >&2
  exit 2
fi
export DOCKER

if ! "$DOCKER" info >/dev/null 2>&1; then
  echo "Docker engine is not ready; trying to start Docker Desktop..."
  powershell -NoProfile -Command "Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'" >/dev/null 2>&1 || true
  for i in $(seq 1 90); do
    if "$DOCKER" info >/dev/null 2>&1; then break; fi
    sleep 2
  done
fi
if ! "$DOCKER" info >/dev/null 2>&1; then
  echo "ERROR: Docker engine is not running. If WSL features were just enabled, reboot Windows first." >&2
  exit 3
fi

mkdir -p artifacts/experiments/docker

echo "=== docker versions ===" | tee artifacts/experiments/docker/versions.log
"$DOCKER" version 2>&1 | tee -a artifacts/experiments/docker/versions.log
"$DOCKER" compose version 2>&1 | tee -a artifacts/experiments/docker/versions.log

echo "=== build ===" | tee artifacts/experiments/docker/build.log
"$DOCKER" compose build 2>&1 | tee -a artifacts/experiments/docker/build.log

echo "=== container version ===" | tee artifacts/experiments/docker/container_version.log
"$DOCKER" compose run --rm tickforge python -m tickforge --version 2>&1 \
  | tee -a artifacts/experiments/docker/container_version.log

echo "=== offline sample replay ===" | tee artifacts/experiments/docker/sample_replay.log
"$DOCKER" compose run --rm tickforge \
  replay --run /app/samples/demo_run --no-derived 2>&1 \
  | tee -a artifacts/experiments/docker/sample_replay.log

echo "=== 60s live run ===" | tee artifacts/experiments/docker/live_60s.log
"$DOCKER" compose run --rm tickforge \
  live --config config/default.toml --duration 60 --out /data --http-port 0 2>&1 \
  | tee -a artifacts/experiments/docker/live_60s.log

RUN_DIR=$(ls -td artifacts/runs/* | head -1)
RUN_ID=$(basename "$RUN_DIR")
echo "container run: $RUN_ID" | tee -a artifacts/experiments/docker/live_60s.log

echo "=== verify ===" | tee artifacts/experiments/docker/verify.log
"$DOCKER" compose run --rm tickforge verify --run "/data/runs/$RUN_ID" 2>&1 \
  | tee -a artifacts/experiments/docker/verify.log

echo "=== deterministic replay ===" | tee artifacts/experiments/docker/replay_verify.log
"$DOCKER" compose run --rm tickforge replay --run "/data/runs/$RUN_ID" --verify 2>&1 \
  | tee -a artifacts/experiments/docker/replay_verify.log

"$DOCKER" image inspect tickforge:0.1.0 \
  > artifacts/experiments/docker/image_inspect.json 2>/dev/null || true

cp "artifacts/runs/$RUN_ID/summary.json" artifacts/experiments/docker/container_summary.json
cp "artifacts/runs/$RUN_ID/replay/replay_summary.json" artifacts/experiments/docker/container_replay_summary.json

echo
echo "Docker verification evidence written to artifacts/experiments/docker/"
echo "run_id=$RUN_ID"
echo "Check: clean_shutdown=true, verify ok=true, replay verified=true"
