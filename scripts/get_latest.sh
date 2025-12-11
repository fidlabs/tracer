#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/ironwolf4/traces"
STATE_DIR="$ROOT/cronstate"
LATEST_FILE="$STATE_DIR/latest_tipset.txt"

cd "$ROOT"

if [[ -f "$LATEST_FILE" ]]; then
  last_str=$(<"$LATEST_FILE")
else
  echo "FATAL: Couldn't find the latest tipset file"
  exit 1
fi

if [[ ! "$last_str" =~ ^[0-9]{12}$ ]]; then
  echo "[ERROR] $LATEST_FILE contains invalid value: '$last_str'" >&2
  exit 1
fi

last=$((10#$last_str))
echo "[INFO] Last known tipset: $last_str"

########################################
# Download latest updates
########################################

# 50,000 is a safe max guess at how far behind we might be
advance=50000
new=$((last + advance))
new_str=$(printf '%012d' "$new")

if [[ ! "$new_str" =~ ^[0-9]{12}$ ]]; then
  echo "[ERROR] new_str '$new_str' is not a 12-digit tipset id" >&2
  exit 1
fi

new=$((10#$new_str))

if (( new <= last )); then
  echo "[INFO] No new tipsets to fetch (target $new_str <= last $last_str)"
  exit 0
fi

# Fetch new range in 10,000-bucketed runs
echo "[STEP] Downloading new tipsets: $last_str+1 .. $new_str"
start_new=$((last + 1))
start_new_str=$(printf '%012d' "$start_new")

python ./beryx.py --start "$start_new_str" --end "$new_str" --failohfail True

