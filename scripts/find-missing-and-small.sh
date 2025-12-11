#!/bin/bash
set -euo pipefail

# Threshold in bytes for "suspiciously small" (default: 1024)
THRESHOLD_BYTES="${1:-1024}"

echo "Finding missing files..."
comm -23 \
  <(seq -f '%012.0f' 1 5588000 | LC_ALL=C sort) \
  <(find 0??? -type f -name 'traces_*.json.s2' -printf '%f\n' \
      | sed 's/^traces_//' \
      | sed 's/\.json\.s2$//' \
      | LC_ALL=C sort) \
  | while read -r id; do
      dir="${id:0:4}/${id:4:4}"
      printf '%s/traces_%s.json.s2\n' "$dir" "$id"
    done > missing-files.txt

echo "Finding suspiciously small files (< ${THRESHOLD_BYTES} bytes)..."
# Output: "<size> <path>"
find 0??? -type f -name 'traces_*.json.s2' -printf '%s %p\n' \
  | awk -v t="$THRESHOLD_BYTES" '$1 < t {print}' \
  > suspicious-small-files.txt

echo "Done."
echo "  Missing files list:        missing-files.txt"
echo "  Suspicious small files:    suspicious-small-files.txt"

