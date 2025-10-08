#!/bin/bash
# Helper script to browse Airflow logs easily

LOGS_DIR="logs/dag_id=filecoin_r2_s2_to_clickhouse"

if [ ! -d "$LOGS_DIR" ]; then
    echo "Logs directory not found: $LOGS_DIR"
    exit 1
fi

echo "=== Recent DAG Runs ==="
ls -lt "$LOGS_DIR" | grep "run_id=" | head -5 | awk '{print $NF}'
echo ""

# Get the latest run
LATEST_RUN=$(ls -t "$LOGS_DIR" | grep "run_id=" | head -1)

if [ -z "$LATEST_RUN" ]; then
    echo "No runs found"
    exit 0
fi

echo "=== Latest Run: $LATEST_RUN ==="
echo ""

# Show task logs
echo "Available tasks:"
ls "$LOGS_DIR/$LATEST_RUN" | grep "task_id=" | sed 's/task_id=/  - /'
echo ""

# If ingest_one exists, show how many tasks
if [ -d "$LOGS_DIR/$LATEST_RUN/task_id=ingest_one" ]; then
    INGEST_COUNT=$(ls "$LOGS_DIR/$LATEST_RUN/task_id=ingest_one" | grep -E '^[0-9]+$' | wc -l)
    echo "ingest_one tasks: $INGEST_COUNT"
    echo ""
fi

# Ask what to view
if [ "$1" == "list_keys" ]; then
    cat "$LOGS_DIR/$LATEST_RUN/task_id=list_keys/attempt=1.log"
elif [ "$1" == "init_schema" ]; then
    cat "$LOGS_DIR/$LATEST_RUN/task_id=init_schema/attempt=1.log"
elif [ "$1" == "dq_checks" ]; then
    cat "$LOGS_DIR/$LATEST_RUN/task_id=dq_checks/attempt=1.log" 2>/dev/null || echo "No dq_checks log found"
elif [ "$1" == "ingest" ] && [ -n "$2" ]; then
    # Show specific ingest_one task
    cat "$LOGS_DIR/$LATEST_RUN/task_id=ingest_one/map_index=$2/attempt=1.log"
else
    echo "Usage: $0 [task]"
    echo ""
    echo "Examples:"
    echo "  $0 list_keys      - View list_keys log"
    echo "  $0 init_schema    - View init_schema log"
    echo "  $0 ingest 0       - View ingest_one task for index 0"
    echo "  $0 ingest 5       - View ingest_one task for index 5"
fi
