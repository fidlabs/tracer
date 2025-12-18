#!/bin/bash
# Helper script to view Airflow task logs from the filesystem

DAG_ID="${1:-trace_seq_ingest_local}"
TASK_ID="${2:-}"
RUN_ID="${3:-latest}"

LOG_ROOT="/mnt/ironwolf1/traces_work/tracer/logs"

if [ -z "$TASK_ID" ]; then
    echo "Usage: $0 <dag_id> <task_id> [run_id]"
    echo ""
    echo "Examples:"
    echo "  $0 trace_seq_ingest_local find_cursor"
    echo "  $0 trace_seq_ingest_local find_cursor latest"
    echo "  $0 trace_seq_ingest_local find_cursor manual__2025-12-18T10:33:05.329511+00:00"
    echo ""
    echo "Available runs for $DAG_ID:"
    ls -1t "$LOG_ROOT/$DAG_ID" 2>/dev/null | head -5
    exit 1
fi

if [ "$RUN_ID" = "latest" ]; then
    RUN_ID=$(ls -1t "$LOG_ROOT/$DAG_ID" 2>/dev/null | head -1)
    if [ -z "$RUN_ID" ]; then
        echo "No runs found for DAG: $DAG_ID"
        exit 1
    fi
    echo "Using latest run: $RUN_ID"
fi

LOG_PATH="$LOG_ROOT/$DAG_ID/$RUN_ID/$TASK_ID/-1/1.log"

if [ ! -f "$LOG_PATH" ]; then
    echo "Log file not found: $LOG_PATH"
    echo ""
    echo "Available tasks for this run:"
    ls -1 "$LOG_ROOT/$DAG_ID/$RUN_ID" 2>/dev/null
    exit 1
fi

echo "=" 
echo "Log: $LOG_PATH"
echo "=" 
echo ""
cat "$LOG_PATH"

