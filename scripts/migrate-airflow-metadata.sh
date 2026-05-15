#!/usr/bin/env bash
# Copy the Airflow metadata database from the legacy shared postgres service to
# postgres-airflow. Run once when upgrading an existing deployment.
#
# Prerequisites:
#   - docker compose stack defined in docker-compose.yml (or -f docker-compose-local-dev.yml)
#   - airflow-scheduler and airflow-webserver STOPPED (avoid writes during cutover)
#   - postgres (legacy) and postgres-airflow both running and healthy
#
# Usage (from repo root):
#   ./scripts/migrate-airflow-metadata.sh
#   COMPOSE_FILE=docker-compose-local-dev.yml ./scripts/migrate-airflow-metadata.sh

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
DUMP_PATH="${DUMP_PATH:-./airflow_metadata.dump}"

echo "==> Using compose file: ${COMPOSE_FILE}"
echo "==> Stopping Airflow services (scheduler/webserver) if running..."
docker compose -f "${COMPOSE_FILE}" stop airflow-scheduler airflow-webserver 2>/dev/null || true

echo "==> Starting postgres services..."
docker compose -f "${COMPOSE_FILE}" up -d postgres postgres-airflow
docker compose -f "${COMPOSE_FILE}" exec postgres bash -c 'until pg_isready -U airflow; do sleep 1; done'
docker compose -f "${COMPOSE_FILE}" exec postgres-airflow bash -c 'until pg_isready -U airflow -d airflow; do sleep 1; done'

echo "==> Backing up filecoin DB (safety copy)..."
docker compose -f "${COMPOSE_FILE}" exec -T postgres \
  pg_dump -U airflow -Fc filecoin > "${DUMP_PATH%.dump}.filecoin-safety.dump" 2>/dev/null \
  || echo "    (filecoin DB not present yet — skipped)"

echo "==> Dumping Airflow metadata from legacy postgres..."
docker compose -f "${COMPOSE_FILE}" exec -T postgres \
  pg_dump -U airflow -Fc airflow > "${DUMP_PATH}"

echo "==> Restoring into postgres-airflow..."
# Drop/recreate airflow DB on the new instance, then restore.
docker compose -f "${COMPOSE_FILE}" exec -T postgres-airflow \
  psql -U airflow -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'airflow' AND pid <> pg_backend_pid();" \
  -c "DROP DATABASE IF EXISTS airflow;" \
  -c "CREATE DATABASE airflow OWNER airflow;"

docker compose -f "${COMPOSE_FILE}" exec -T postgres-airflow \
  pg_restore -U airflow -d airflow --no-owner --role=airflow < "${DUMP_PATH}"

echo "==> Running airflow db migrate on new metadata DB..."
docker compose -f "${COMPOSE_FILE}" run --rm airflow-init

echo "==> Done. Start Airflow with:"
echo "    docker compose -f ${COMPOSE_FILE} up -d airflow-scheduler airflow-webserver"
echo "Verify DAG run history in the UI, then archive ${DUMP_PATH} if satisfied."
