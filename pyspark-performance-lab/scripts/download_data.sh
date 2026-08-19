#!/usr/bin/env bash
set -euo pipefail

YEAR="${1:-2025}"
START_MONTH="${2:-1}"
END_MONTH="${3:-3}"

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required on the host." >&2
  exit 1
fi

if (( START_MONTH < 1 || START_MONTH > 12 || END_MONTH < 1 || END_MONTH > 12 || START_MONTH > END_MONTH )); then
  echo "Usage: $0 [YEAR] [START_MONTH] [END_MONTH]" >&2
  echo "Example: $0 2025 1 3" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="$ROOT_DIR/data/raw/yellow"
LOOKUP_DIR="$ROOT_DIR/data/lookup"
mkdir -p "$RAW_DIR" "$LOOKUP_DIR"

BASE_URL="https://d37ci6vzurychx.cloudfront.net"

printf 'Downloading official NYC TLC Yellow Taxi data: %s months %s..%s\n' "$YEAR" "$START_MONTH" "$END_MONTH"
for month in $(seq "$START_MONTH" "$END_MONTH"); do
  mm=$(printf '%02d' "$month")
  filename="yellow_tripdata_${YEAR}-${mm}.parquet"
  url="$BASE_URL/trip-data/$filename"
  destination="$RAW_DIR/$filename"

  if [[ -s "$destination" ]]; then
    echo "SKIP: $filename already exists"
    continue
  fi

  echo "GET : $url"
  curl --fail --location --retry 4 --retry-delay 2 --continue-at - \
    --output "$destination" "$url"
done

lookup="$LOOKUP_DIR/taxi_zone_lookup.csv"
if [[ ! -s "$lookup" ]]; then
  echo "GET : $BASE_URL/misc/taxi_zone_lookup.csv"
  curl --fail --location --retry 4 --retry-delay 2 \
    --output "$lookup" "$BASE_URL/misc/taxi_zone_lookup.csv"
else
  echo "SKIP: taxi_zone_lookup.csv already exists"
fi

echo
echo "Downloaded files:"
ls -lh "$RAW_DIR"/*.parquet "$lookup"
echo
echo "Total local dataset size:"
du -sh "$RAW_DIR" "$LOOKUP_DIR"
