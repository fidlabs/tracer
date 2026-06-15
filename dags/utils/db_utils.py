"""Database connection and state tracking utilities."""
import os
import clickhouse_connect
import psycopg


def get_clickhouse_client(host: str = None, port: int = None, username: str = None, password: str = None):
    """Create and return a ClickHouse client."""
    return clickhouse_connect.get_client(
        host=host or os.getenv("CH_HOST", "clickhouse"),
        port=port or int(os.getenv("CH_PORT", "8123")),
        username=username or os.getenv("CLICKHOUSE_USER", "default"),
        password=password or os.getenv("CLICKHOUSE_PASSWORD", "default"),
    )


def get_postgres_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "filecoin"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        connect_timeout=10
    )


def claim_file_processing(ch_client, seq_num: int, etag: str) -> tuple[bool, int]:
    """
    Claim file for processing in seq_load_state.
    
    Returns:
        (should_process, attempts): tuple indicating if file should be processed and attempt count
    """
    # Escape helper for single-quoted SQL
    def esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")
    
    # Check if already successfully processed
    check_result = ch_client.query(f"""
        SELECT status, attempts 
        FROM filecoin.seq_load_state 
        WHERE seq_num={seq_num} AND etag='{esc(etag)}' AND status='success'
    """)
    
    if check_result.result_rows:
        return False, 0  # Already processed, skip
    
    # Get current attempt count
    attempt_result = ch_client.query(f"""
        SELECT max(attempts) 
        FROM filecoin.seq_load_state 
        WHERE seq_num={seq_num}
    """)
    attempts = (attempt_result.result_rows[0][0] if attempt_result.result_rows and attempt_result.result_rows[0][0] else 0) + 1
    
    # DELETE old 'running' record if exists, then INSERT new one
    ch_client.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq_num} AND status='running'")
    ch_client.command(f"""
        INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
        VALUES ({seq_num}, '{esc(etag)}', 'running', {attempts}, '', now(), NULL)
    """)
    
    return True, attempts


def mark_file_success(ch_client, seq_num: int, etag: str, attempts: int):
    """Mark file as successfully processed in seq_load_state."""
    def esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")
    
    # DELETE old record and INSERT success record (ReplacingMergeTree pattern)
    ch_client.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq_num} AND status='running'")
    ch_client.command(f"""
        INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
        VALUES ({seq_num}, '{esc(etag)}', 'success', {attempts}, '', now(), now())
    """)


def mark_file_failed(ch_client, seq_num: int, etag: str, attempts: int, error: str):
    """Mark file as failed in seq_load_state."""
    def esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")
    
    err = esc(str(error))[:2048]  # Limit error message length
    
    # DELETE old record and INSERT failed record
    ch_client.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq_num} AND status='running'")
    ch_client.command(f"""
        INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
        VALUES ({seq_num}, '{esc(etag)}', 'failed', {attempts}, '{err}', now(), NULL)
    """)

