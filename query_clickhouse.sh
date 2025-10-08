#!/bin/bash
# Quick ClickHouse query helper

CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-8123}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-default}"

# If no query provided, show interactive help
if [ -z "$1" ]; then
    echo "Usage: $0 [query|stats|sample|recent]"
    echo ""
    echo "Examples:"
    echo "  $0 stats           - Show database statistics"
    echo "  $0 sample          - Show sample messages"
    echo "  $0 recent          - Show recently ingested messages"
    echo "  $0 'SELECT ...'    - Run custom query"
    echo ""
    exit 0
fi

# Function to run query
query() {
    curl -s "http://${CH_HOST}:${CH_PORT}/?user=${CH_USER}&password=${CH_PASS}" \
         --data-binary "$1" | column -t -s $'\t'
}

case "$1" in
    stats)
        echo "=== Database Statistics ==="
        query "
        SELECT 
            'Messages' as table, 
            count() as rows, 
            formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
            formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size
        FROM system.parts 
        WHERE database = 'filecoin' AND table = 'messages' AND active
        UNION ALL
        SELECT 
            'Subcalls' as table,
            count() as rows,
            formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
            formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size
        FROM system.parts 
        WHERE database = 'filecoin' AND table = 'subcalls' AND active
        UNION ALL
        SELECT
            'Loaded Keys' as table,
            count() as rows,
            '' as compressed_size,
            '' as uncompressed_size
        FROM filecoin.loaded_keys
        "
        echo ""
        echo "=== Processing Status ==="
        query "
        SELECT 
            status,
            count() as count,
            formatReadableSize(sum(size_bytes)) as total_size
        FROM filecoin.loaded_keys 
        GROUP BY status 
        ORDER BY status
        "
        ;;
    
    sample)
        echo "=== Sample Messages (5 rows) ==="
        query "
        SELECT 
            msg_cid,
            from_addr,
            to_addr,
            toString(value_atto) as value_atto,
            method,
            rct_exit_code,
            toString(total_cost) as total_cost
        FROM filecoin.messages 
        LIMIT 5
        "
        ;;
    
    recent)
        echo "=== Recently Ingested Messages (10 rows) ==="
        query "
        SELECT 
            msg_cid,
            from_addr,
            to_addr,
            toString(value_atto) as value_atto,
            method,
            rct_exit_code,
            ingested_at
        FROM filecoin.messages 
        ORDER BY ingested_at DESC 
        LIMIT 10
        "
        ;;
    
    *)
        # Custom query
        query "$1"
        ;;
esac

