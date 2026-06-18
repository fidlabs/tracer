#!/bin/bash
# Helper script to reset processed files tracking in ClickHouse

set -e

# Default ClickHouse connection
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-8123}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASS="${CLICKHOUSE_PASSWORD:-default}"

# Function to execute ClickHouse query
execute_query() {
    local query="$1"
    curl -s "http://${CH_HOST}:${CH_PORT}/?user=${CH_USER}&password=${CH_PASS}" \
         --data-binary "${query}"
    echo ""
}

# Show usage
show_usage() {
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  all              - Clear ALL processed files (full reset)"
    echo "  success          - Clear only successfully processed files"
    echo "  failed           - Clear only failed files"
    echo "  range START END  - Clear files in sequence range (e.g., range 1 100)"
    echo "  stats            - Show processing statistics"
    echo ""
}

# Show stats
show_stats() {
    echo "=== Processing Statistics ==="
    execute_query "SELECT status, count() as count, sum(size_bytes) as total_bytes FROM filecoin.loaded_keys GROUP BY status ORDER BY status"
    echo ""
    echo "=== Latest Processed Files ==="
    execute_query "SELECT key, status, attempts, processed_at FROM filecoin.loaded_keys ORDER BY processed_at DESC LIMIT 10"
}

# Main logic
case "${1:-}" in
    all)
        echo "Clearing ALL processed files..."
        execute_query "TRUNCATE TABLE filecoin.loaded_keys"
        echo "✓ All records cleared"
        ;;
    success)
        echo "Clearing successfully processed files..."
        execute_query "DELETE FROM filecoin.loaded_keys WHERE status = 'success'"
        echo "✓ Success records cleared"
        ;;
    failed)
        echo "Clearing failed files..."
        execute_query "DELETE FROM filecoin.loaded_keys WHERE status = 'failed'"
        echo "✓ Failed records cleared"
        ;;
    range)
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Error: range requires START and END parameters"
            show_usage
            exit 1
        fi
        START=$2
        END=$3
        echo "Clearing files in sequence range ${START} to ${END}..."
        execute_query "DELETE FROM filecoin.loaded_keys WHERE key LIKE 'traces/traces_%' AND toUInt64(splitByChar('_', splitByChar('.', key)[1])[2]) BETWEEN ${START} AND ${END}"
        echo "✓ Range records cleared"
        ;;
    stats)
        show_stats
        ;;
    *)
        show_usage
        exit 1
        ;;
esac

