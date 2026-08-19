#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rm -rf "$ROOT_DIR/output/baseline"/* "$ROOT_DIR/output/optimized"/* "$ROOT_DIR/data/prepared"/* "$ROOT_DIR/spark-events"/*
rm -f "$ROOT_DIR/benchmark/results.csv"
echo "Generated outputs, prepared data, event logs, and benchmark CSV were cleared. Raw TLC data was kept."
