#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <python-file-relative-to-project> [job args...]" >&2
  echo "Example: $0 jobs/00_smoke_test.py" >&2
  exit 1
fi

JOB="$1"
shift

PROJECT_PATH="/opt/spark/work-dir/project"

docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  "$PROJECT_PATH/$JOB" "$@"
