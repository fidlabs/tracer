from __future__ import annotations
import os, shlex, subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from airflow import DAG
from airflow.decorators import task

# --- config ---
TRACES_ROOT   = Path(os.getenv("TRACES_ROOT", "/data/traces"))  # mounted host dir
CH_HTTP       = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST       = os.getenv("CH_HOST", "clickhouse")
CH_PORT       = int(os.getenv("CH_PORT", "8123"))
CH_USER       = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD   = os.getenv("CLICKHOUSE_PASSWORD", "default")
START_SEQ     = int(os.getenv("START_SEQ", "1"))     # where to begin if no state
BATCH_MAX     = int(os.getenv("BATCH_MAX", "1024"))  # max files per run, Airflow limit for maps is 1024
SHELL         = "bash"                               # for shell pipelines

def key_from_seq(n: int) -> Path:
    """
    Bucketed directories of 10,000 files:
      top = n // 100_000_000  -> 0000 for current range
      mid = n // 10_000       -> e.g. 0000, 0553
      file name = traces_<12d>.json.s2
    """
    z = f"{n:012d}"
    top = n // 100_000_000
    mid = n // 10_000
    return TRACES_ROOT / f"{top:04d}" / f"{mid:04d}" / f"traces_{z}.json.s2"

def etag_for_file(p: Path) -> str:
    # Cheap versioning: size + mtime_ns; fast and good enough to detect changes.
    st = p.stat()
    return f"{st.st_size}-{st.st_mtime_ns}"

def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )

def ch_url(sql: str) -> str:
    # HTTP endpoint with auth; SQL is passed as a query parameter, data via POST body.
    return f"{CH_HTTP}/?user={quote(CH_USER)}&password={quote(CH_PASSWORD)}&query={quote(sql)}"

def insert_sql_for_seq(n: int) -> str:
    # Each emitted line is one element from .Trace[] (a JSON object with "Msg": {...})
    return f"""
    INSERT INTO filecoin.transactions_raw
    SELECT
      {n} AS seq_num,
      toUInt8(JSONExtractUInt(line,'Msg','Version'))              AS msg_version,
      JSONExtractString(line,'Msg','To')                          AS to_addr,
      JSONExtractString(line,'Msg','From')                        AS from_addr,
      toDecimal128OrZero(JSONExtractString(line,'Msg','Value'),38) AS value_atto,
      toUInt32(JSONExtractUInt(line,'Msg','Method'))              AS method,
      assumeNotNull(JSONExtractString(line,'Msg','Params'))       AS params_b64,
      JSONExtractString(line,'Msg','CID','/')                     AS msg_cid,
      now()                                                       AS ingested_at
    FROM input('line String')
    FORMAT LineAsString
    """.strip()

with DAG(
    dag_id="trace_seq_ingest_local",
    start_date=datetime(2024, 1, 1),
    schedule="*/3 * * * *",  # Run every 3 minutes
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","s2","clickhouse","seq","local"]
) as dag:

    @task
    def find_cursor() -> int:
        print("=" * 60)
        print("TASK: find_cursor")
        print("=" * 60)
        print(f"Connecting to ClickHouse at {CH_HOST}:{CH_PORT}...")
        c = ch_client()
        print("Querying max(seq_num) from filecoin.seq_load_state WHERE status='success'...")
        result = c.query("SELECT max(seq_num) FROM filecoin.seq_load_state WHERE status='success'")
        row = result.result_rows[0][0] if result.result_rows else None
        cursor = int(row) if row is not None else (START_SEQ - 1)
        if row is None:
            print(f"✓ No previous successful loads found. Starting from seq {START_SEQ} (cursor={cursor})")
        else:
            print(f"✓ Found last successful seq_num: {row}")
            print(f"✓ Next batch will start from seq {cursor + 1}")
        print(f"Returning cursor: {cursor}")
        return cursor

    @task
    def plan_next_batch(prev: int) -> list[dict]:
        """
        Walk forward from prev+1, check local file exists, stop on first gap.
        Return up to BATCH_MAX present files with 'etag' (size+mtime).
        """
        print("=" * 60)
        print("TASK: plan_next_batch")
        print("=" * 60)
        print(f"Previous cursor: {prev}")
        print(f"Starting from seq: {prev + 1}")
        print(f"Max batch size: {BATCH_MAX}")
        print(f"Traces root: {TRACES_ROOT}")
        print()
        
        out = []
        seq = prev + 1
        checked = 0
        for i in range(BATCH_MAX):
            p = key_from_seq(seq)
            checked += 1
            if not p.exists():
                print(f"  Gap detected at seq {seq}: {p} not found")
                break  # stop at first hole
            etag = etag_for_file(p)
            out.append({"seq": seq, "path": str(p), "etag": etag})
            if (i + 1) % 100 == 0:
                print(f"  Progress: checked {i + 1}/{BATCH_MAX}, found {len(out)} files so far...")
            seq += 1
        
        print()
        print(f"✓ Checked {checked} sequence numbers")
        print(f"✓ Found {len(out)} files to process")
        if len(out) > 0:
            print(f"✓ Range: seq {out[0]['seq']} to {out[-1]['seq']}")
        else:
            print("⚠ No files found in this batch")
        return out

    @task
    def ingest_one(obj: dict) -> str:
        """
        Claim seq -> s2dec -> jq -c '.Trace[]' -> HTTP INSERT -> mark success/fail.
        Requires 's2dec', 'jq', 'curl' in PATH inside the image.
        """
        seq  = obj["seq"]
        path = obj["path"]
        etag = obj["etag"]
        
        print(f"[seq {seq}] Starting ingestion...")
        print(f"[seq {seq}] File: {path}")
        print(f"[seq {seq}] ETag: {etag}")

        # escape helper for single-quoted SQL
        def esc(s: str) -> str: return s.replace("\\","\\\\").replace("'","\\'")

        print(f"[seq {seq}] Connecting to ClickHouse...")
        c = ch_client()

        # CLAIM: only if not already success for same (seq, etag)
        print(f"[seq {seq}] Checking if already processed...")
        check_result = c.query(f"""
            SELECT status, attempts 
            FROM filecoin.seq_load_state 
            WHERE seq_num={seq} AND etag='{esc(etag)}' AND status='success'
        """)
        
        if check_result.result_rows:
            print(f"[seq {seq}] ⏭ SKIPPED: Already successfully processed with this ETag")
            return f"skip {seq} (already success)"
        
        # Get current attempt count
        attempt_result = c.query(f"""
            SELECT max(attempts) 
            FROM filecoin.seq_load_state 
            WHERE seq_num={seq}
        """)
        attempts = (attempt_result.result_rows[0][0] if attempt_result.result_rows and attempt_result.result_rows[0][0] else 0) + 1
        
        print(f"[seq {seq}] Claiming task in seq_load_state (attempt #{attempts})...")
        # DELETE old 'running' record if exists, then INSERT new one
        c.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq} AND status='running'")
        c.command(f"""
            INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
            VALUES ({seq}, '{esc(etag)}', 'running', {attempts}, '', now(), NULL)
        """)
        print(f"[seq {seq}] ✓ Task claimed")
        print(f"[seq {seq}] Building pipeline: s2d -> jq -> curl -> ClickHouse...")

        # stream: s2d -> compact JSON lines -> ClickHouse HTTP insert
        # s2d -c <file>  => decompress to stdout
        src  = f"/usr/local/bin/s2d -c {shlex.quote(path)} | /usr/bin/jq -c '.Trace[]'"
        curl = f"curl -sS '{ch_url(insert_sql_for_seq(seq))}' --data-binary @-"

        try:
            print(f"[seq {seq}] Executing pipeline...")
            proc = subprocess.run(
                f"{src} | {curl}",
                shell=True,
                executable=SHELL,
                capture_output=True,
                text=True
            )
            
            if proc.returncode != 0:
                print(f"[seq {seq}] ❌ Pipeline failed!")
                print(f"[seq {seq}] Return code: {proc.returncode}")
                print(f"[seq {seq}] STDERR: {proc.stderr}")
                print(f"[seq {seq}] STDOUT: {proc.stdout}")
                raise RuntimeError(f"pipeline failed for seq {seq}: {proc.stderr}")

            print(f"[seq {seq}] ✓ Pipeline completed successfully")
            print(f"[seq {seq}] Marking status as 'success'...")

            # SUCCESS: DELETE old record and INSERT success record (ReplacingMergeTree pattern)
            c.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq} AND status='running'")
            c.command(f"""
                INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
                VALUES ({seq}, '{esc(etag)}', 'success', {attempts}, '', now(), now())
            """)
            print(f"[seq {seq}] ✅ SUCCESS: Marked as completed")
            return f"ok {seq}"

        except Exception as e:
            print(f"[seq {seq}] ❌ ERROR: {e}")
            err = esc(str(e))[:2048]
            print(f"[seq {seq}] Marking status as 'failed' with error message...")
            # FAILED: DELETE old record and INSERT failed record
            c.command(f"DELETE FROM filecoin.seq_load_state WHERE seq_num={seq} AND status='running'")
            c.command(f"""
                INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
                VALUES ({seq}, '{esc(etag)}', 'failed', {attempts}, '{err}', now(), NULL)
            """)
            raise

    @task
    def route_special(_results: list[str]) -> None:
        """
        Stub: later, replace with logic to enqueue to filecoin.special_queue
        or branch to per-actor handlers based on (to_addr, method).
        """
        print("=" * 60)
        print("TASK: route_special")
        print("=" * 60)
        print(f"Received {len(_results)} ingestion results")
        
        # Count successes vs skips
        ok_count = sum(1 for r in _results if r.startswith("ok"))
        skip_count = sum(1 for r in _results if r.startswith("skip"))
        fail_count = len(_results) - ok_count - skip_count
        
        print(f"  ✓ Successful: {ok_count}")
        print(f"  ⏭ Skipped: {skip_count}")
        if fail_count > 0:
            print(f"  ❌ Failed: {fail_count}")
        
        print()
        print("⚠ Special processing stub: TODO")
        print("  Future: enqueue to filecoin.special_queue or branch to per-actor handlers")
        print("=" * 60)

    prev = find_cursor()
    batch = plan_next_batch(prev)
    results = ingest_one.expand(obj=batch)
    route = route_special(results)

    prev >> batch >> results >> route
