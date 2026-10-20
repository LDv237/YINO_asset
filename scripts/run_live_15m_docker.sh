#!/usr/bin/env bash
# Build the image and run the 15-minute measurement inside Docker.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose build
docker compose up --abort-on-container-exit
