#!/usr/bin/env bash
# Reset trace_seq_beryx_parser progress so reprocessing starts at cursor+1.
#
# - ClickHouse filecoin.seq_load_state: REQUIRED (otherwise claim_file_processing skips files)
# - Postgres filecoin DB: recommended deals cleanup; optional height-keyed tables
#
# Usage (from repo root):
#   ./scripts/reset-cursor.sh 5992075
#   ./scripts/reset-cursor.sh 5992075 --dry-run
#   COMPOSE_FILE=docker-compose-local-dev.yml ./scripts/reset-cursor.sh 5992075
#
# Options:
#   --dry-run           Show inspection only; no writes
#   --yes               Skip confirmation prompts (use with care)
#   --skip-postgres     Only reset ClickHouse load state
#   --skip-airflow-stop Do not stop scheduler/webserver first

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
CURSOR_SEQ=""
DRY_RUN=0
ASSUME_YES=0
SKIP_POSTGRES=0
SKIP_AIRFLOW_STOP=0

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage 0 ;;
    --dry-run) DRY_RUN=1 ;;
    --yes) ASSUME_YES=1 ;;
    --skip-postgres) SKIP_POSTGRES=1 ;;
    --skip-airflow-stop) SKIP_AIRFLOW_STOP=1 ;;
    -*)
      echo "Unknown option: $1" >&2
      usage 1
      ;;
    *)
      if [[ -z "${CURSOR_SEQ}" ]]; then
        CURSOR_SEQ="$1"
      else
        echo "Unexpected argument: $1" >&2
        usage 1
      fi
      ;;
  esac
  shift
done

if [[ -z "${CURSOR_SEQ}" ]] || ! [[ "${CURSOR_SEQ}" =~ ^[0-9]+$ ]]; then
  echo "Error: CURSOR_SEQ (last successful seq_num) is required, e.g. ./scripts/reset-cursor.sh 5992075" >&2
  usage 1
fi

NEXT_SEQ=$((CURSOR_SEQ + 1))

dc() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

ch_query() {
  dc exec -T clickhouse clickhouse-client --multiquery "$@"
}

pg_query() {
  # psql in the postgres:15 image does not support --query; read SQL from stdin.
  dc exec -T postgres psql -U airflow -d filecoin -v ON_ERROR_STOP=1
}

confirm() {
  local prompt="$1"
  if [[ "${ASSUME_YES}" -eq 1 ]]; then
    return 0
  fi
  read -r -p "${prompt} [y/N] " reply
  [[ "${reply}" =~ ^[Yy]$ ]]
}

echo "==> Compose file: ${COMPOSE_FILE}"
echo "==> Cursor (last successful seq_num): ${CURSOR_SEQ}"
echo "==> Reprocessing will start at seq: ${NEXT_SEQ}"
[[ "${DRY_RUN}" -eq 1 ]] && echo "==> DRY RUN (no writes)"

if [[ "${SKIP_AIRFLOW_STOP}" -eq 0 ]]; then
  echo "==> Stopping Airflow scheduler/webserver..."
  if [[ "${DRY_RUN}" -eq 0 ]]; then
    dc stop airflow-scheduler airflow-webserver 2>/dev/null || true
  else
    echo "    (dry-run: would stop airflow-scheduler airflow-webserver)"
  fi
fi

echo "==> Ensuring postgres and clickhouse are up..."
if [[ "${DRY_RUN}" -eq 0 ]]; then
  dc up -d postgres clickhouse
  dc exec postgres bash -c 'until pg_isready -U airflow; do sleep 1; done'
  dc exec clickhouse bash -c "until wget -qO- http://localhost:8123/ping | grep -q Ok; do sleep 1; done"
fi

echo ""
echo "=== ClickHouse inspection ==="
ch_query --query "
SELECT max(seq_num) AS current_cursor
FROM filecoin.seq_load_state FINAL
WHERE status = 'success';

SELECT status, count() AS cnt, min(seq_num) AS min_seq, max(seq_num) AS max_seq
FROM filecoin.seq_load_state FINAL
WHERE seq_num > ${CURSOR_SEQ}
GROUP BY status
ORDER BY status;
"

if [[ "${SKIP_POSTGRES}" -eq 0 ]]; then
  echo ""
  echo "=== Postgres inspection (rows that would be deleted) ==="
  pg_query <<SQL
SELECT 'deals' AS tbl, count(*)::bigint AS cnt FROM public.deals WHERE "termStart" > ${CURSOR_SEQ}
UNION ALL
SELECT 'verifier_allowance', count(*) FROM public.verifier_allowance WHERE height > ${CURSOR_SEQ}
UNION ALL
SELECT 'verified_client_allowance', count(*) FROM public.verified_client_allowance WHERE height > ${CURSOR_SEQ}
UNION ALL
SELECT 'virtual_verifier_allowance', count(*) FROM public.virtual_verifier_allowance WHERE height > ${CURSOR_SEQ}
UNION ALL
SELECT 'virtual_verified_client_allowance', count(*) FROM public.virtual_verified_client_allowance WHERE height > ${CURSOR_SEQ}
UNION ALL
SELECT 'sector_activations', count(*) FROM public.sector_activations WHERE "activationHeight" > ${CURSOR_SEQ};
SQL
fi

if [[ "${DRY_RUN}" -eq 1 ]]; then
  echo ""
  echo "Dry run complete. Re-run without --dry-run to apply changes."
  exit 0
fi

echo ""
echo "=== ClickHouse reset ==="
echo "Will DELETE all seq_load_state rows with seq_num > ${CURSOR_SEQ}"
if confirm "Proceed with ClickHouse DELETE?"; then
  ch_query --query "
ALTER TABLE filecoin.seq_load_state
DELETE WHERE seq_num > ${CURSOR_SEQ};
"
  echo "Waiting for ClickHouse mutation to finish..."
  for _ in $(seq 1 120); do
    pending=$(ch_query --query "
SELECT count()
FROM system.mutations
WHERE database = 'filecoin'
  AND table = 'seq_load_state'
  AND is_done = 0;
" | tail -n 1 | tr -d '[:space:]')
    if [[ "${pending}" == "0" ]]; then
      break
    fi
    sleep 2
  done
  ch_query --query "OPTIMIZE TABLE filecoin.seq_load_state FINAL;"
  echo "ClickHouse cursor after reset:"
  ch_query --query "
SELECT max(seq_num) AS cursor_after
FROM filecoin.seq_load_state FINAL
WHERE status = 'success';
"
else
  echo "Skipped ClickHouse reset."
  exit 1
fi

if [[ "${SKIP_POSTGRES}" -eq 0 ]]; then
  echo ""
  echo "=== Postgres cleanup ==="
  echo "Recommended: delete deals (not idempotent on re-run)."
  echo "Optional: delete height-keyed allowance / sector_activation rows."
  if confirm "Delete Postgres rows with height/termStart > ${CURSOR_SEQ}?"; then
    pg_query <<SQL
BEGIN;
DELETE FROM public.deals WHERE "termStart" > ${CURSOR_SEQ};
DELETE FROM public.verifier_allowance WHERE height > ${CURSOR_SEQ};
DELETE FROM public.virtual_verifier_allowance WHERE height > ${CURSOR_SEQ};
DELETE FROM public.verified_client_allowance WHERE height > ${CURSOR_SEQ};
DELETE FROM public.virtual_verified_client_allowance WHERE height > ${CURSOR_SEQ};
DELETE FROM public.sector_activations WHERE "activationHeight" > ${CURSOR_SEQ};
COMMIT;
SQL
    echo "Postgres cleanup committed (allocations left unchanged — ON CONFLICT safe)."
  else
    echo "Skipped Postgres cleanup."
  fi
fi

echo ""
echo "==> Done."
echo "    Next: clear failed DAG runs in the Airflow UI if needed, then:"
echo "    docker compose -f ${COMPOSE_FILE} up -d airflow-scheduler airflow-webserver"
echo "    find_cursor_task should log: Next batch will start from seq ${NEXT_SEQ}"
