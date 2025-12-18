from __future__ import annotations
import os, shlex, subprocess
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
import boto3
import clickhouse_connect
from urllib.parse import quote

CH_HTTP = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))

# Processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))  # Number of files to process per run
START_FROM = int(os.getenv("START_FROM", "1"))    # Starting file number (1-based)
MAX_MISSING_WAIT = int(os.getenv("MAX_MISSING_WAIT", "10"))  # Max consecutive missing files before stopping

SHELL = "bash"

def ch():
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username="default", password="default")

def _curl_insert(sql: str) -> str:
    # For input() format, we send SQL via query param and data via POST body
    # But we need to be careful about URL length limits
    return f"{CH_HTTP}/?user=default&password=default"

def _sql_insert_messages() -> str:
    return """
INSERT INTO filecoin.messages
SELECT
  JSONExtractString(line,'MsgCid','/')                         AS msg_cid,
  toUInt8(JSONExtractUInt(line,'Msg','Version'))               AS msg_version,
  JSONExtractString(line,'Msg','To')                           AS to_addr,
  JSONExtractString(line,'Msg','From')                         AS from_addr,
  toUInt64(JSONExtractUInt(line,'Msg','Nonce'))                AS nonce,
  toDecimal128OrZero(JSONExtractString(line,'Msg','Value'),38) AS value_atto,
  toUInt64(JSONExtractUInt(line,'Msg','GasLimit'))             AS gas_limit,
  toDecimal128OrZero(JSONExtractString(line,'Msg','GasFeeCap'),38)   AS gas_fee_cap,
  toDecimal128OrZero(JSONExtractString(line,'Msg','GasPremium'),38)  AS gas_premium,
  toUInt32(JSONExtractUInt(line,'Msg','Method'))               AS method,
  JSONExtractString(line,'Msg','Params')                       AS params_b64,
  toInt32(JSONExtractInt(line,'MsgRct','ExitCode'))            AS rct_exit_code,
  JSONExtractString(line,'MsgRct','Return')                    AS rct_return_b64,
  toUInt64(JSONExtractUInt(line,'MsgRct','GasUsed'))           AS rct_gas_used,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','BaseFeeBurn'),38)      AS base_fee_burn,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','OverEstimationBurn'),38)AS overestimation_burn,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','MinerPenalty'),38)      AS miner_penalty,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','MinerTip'),38)          AS miner_tip,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','Refund'),38)            AS refund,
  toDecimal128OrZero(JSONExtractString(line,'GasCost','TotalCost'),38)         AS total_cost,
  toUInt64(JSONExtractUInt(line,'ExecutionTrace','Duration'))  AS trace_duration_ns,
  JSONExtractString(line,'Error')                               AS trace_error,
  now()                                                         AS ingested_at
FROM input('line String')
FORMAT LineAsString
""".strip()

def _sql_insert_subcalls() -> str:
    return """
INSERT INTO filecoin.subcalls
SELECT
  JSONExtractString(line,'MsgCid','/')                           AS parent_cid,
  idx                                                            AS idx,
  JSONExtractString(sub,'Msg','To')                              AS to_addr,
  JSONExtractString(sub,'Msg','From')                            AS from_addr,
  toUInt32(JSONExtractUInt(sub,'Msg','Method'))                  AS method,
  toInt32(JSONExtractInt(sub,'MsgRct','ExitCode'))               AS exit_code,
  JSONExtractString(sub,'MsgRct','Return')                       AS return_b64,
  toUInt64(JSONExtractUInt(sub,'MsgRct','GasUsed'))              AS gas_used,
  toUInt64(JSONExtractUInt(sub,'Duration'))                      AS duration_ns,
  now()                                                          AS ingested_at
FROM (
  SELECT _line AS line,
         JSONExtractArrayRaw(_line,'ExecutionTrace','Subcalls') AS subs
  FROM input(' _line String ')
)
ARRAY JOIN subs AS sub, arrayEnumerate(subs) AS idx
FORMAT LineAsString
""".strip()

with DAG(
    dag_id="local_to_clickhouse",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","clickhouse","exactly-once-ish"]
) as dag:

    @task
    def init_schema():
        with open("/opt/airflow/include/sql/00_schema.sql","r",encoding="utf-8") as f:
            ddl = f.read()
        c = ch()
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            c.command(stmt)

    @task
    def list_keys() -> list[dict]:
        """Generate sequential file keys based on deterministic naming pattern.
        
        Files are named: traces_000000000001.json.s2, traces_000000000002.json.s2, etc.
        This function generates BATCH_SIZE sequential keys starting from START_FROM.
        Skips files that have already been successfully processed (immutable files).
        """
        s3 = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            endpoint_url=R2_ENDPOINT,
            region_name="auto",
        )
        
        c = ch()
        
        # Get the set of already successfully processed keys
        print("Checking for already processed files...")
        processed_keys_query = """
            SELECT key 
            FROM filecoin.loaded_keys 
            WHERE status = 'success'
        """
        processed_keys_result = c.query(processed_keys_query)
        processed_keys = set(row[0] for row in processed_keys_result.result_rows)
        print(f"Found {len(processed_keys)} already processed files")
        
        keys = []
        missing_count = 0
        current_num = START_FROM
        skipped_count = 0
        
        print(f"Starting sequential processing: batch_size={BATCH_SIZE}, start_from={START_FROM}")
        print(f"Looking for files: traces_XXXXXXXXXXXX.json.s2 format")
        
        # Process BATCH_SIZE files sequentially
        for i in range(BATCH_SIZE):
            # Generate the deterministic filename
            filename = f"traces_{current_num:012d}.json.s2"
            key = f"{R2_PREFIX}traces/{filename}" if R2_PREFIX else f"traces/{filename}"
            
            # Check if already processed
            if key in processed_keys:
                print(f"⊙ Already processed {filename} - skipping")
                skipped_count += 1
                current_num += 1
                continue
            
            try:
                # Check if the file exists by trying to get its metadata
                response = s3.head_object(Bucket=R2_BUCKET, Key=key)
                
                # File exists, add it to our processing list
                lm = response.get("LastModified")
                if hasattr(lm, "timestamp"):
                    lm_ts = datetime.fromtimestamp(lm.timestamp(), tz=timezone.utc)
                else:
                    lm_ts = datetime.now(timezone.utc)
                    
                keys.append({
                    "key": key,
                    "etag": response.get("ETag", "").strip('"'),
                    "size": int(response.get("ContentLength", 0)),
                    "last_modified": lm_ts.isoformat(),
                    "sequence_number": current_num
                })
                
                print(f"✓ Found {filename} ({response.get('ContentLength', 0)} bytes)")
                missing_count = 0  # Reset missing counter
                
            except s3.exceptions.NoSuchKey:
                # File doesn't exist
                missing_count += 1
                print(f"✗ Missing {filename} (missing count: {missing_count})")
                
                # If we hit too many consecutive missing files, stop processing
                if missing_count >= MAX_MISSING_WAIT:
                    print(f"Stopping: {missing_count} consecutive missing files (max: {MAX_MISSING_WAIT})")
                    break
                    
            except Exception as e:
                print(f"Error checking {filename}: {str(e)}")
                missing_count += 1
                if missing_count >= MAX_MISSING_WAIT:
                    break
            
            current_num += 1
        
        print(f"Completed: found {len(keys)} new files to process, {skipped_count} already processed, stopped at sequence {current_num}")
        return keys

    @task
    def ingest_one(obj: dict) -> str:
        """
        Claim → stream/decode → insert → mark success/fail.
        Never reprocess (key,etag) if already success.
        """
        key = obj["key"]
        etag = obj["etag"]
        size = obj["size"]
        lm   = obj["last_modified"]

        def esc(s: str) -> str:
            return s.replace("\\", "\\\\").replace("'", "\\'")

        c = ch()

        # --- CLAIM (atomic insert if no success exists for same key+etag) ---
        claim_sql = f"""
        INSERT INTO filecoin.loaded_keys (key, etag, size_bytes, last_modified, status, attempts, last_error, first_seen, updated_at, processed_at)
        SELECT '{esc(key)}', '{esc(etag)}', {size}, parseDateTimeBestEffort('{esc(lm)}'), 'running',
               ifNull( (SELECT max(attempts) FROM filecoin.loaded_keys WHERE key = '{esc(key)}'), 0) + 1,
               '', now(), now(), NULL
        WHERE NOT EXISTS (
            SELECT 1 FROM filecoin.loaded_keys
            WHERE key = '{esc(key)}' AND etag = '{esc(etag)}' AND status = 'success'
        )
        """
        inserted = c.command(claim_sql)  # returns rows affected (0 or 1)
        if inserted == 0:
            return f"skip (already success) {key}"

        # build common source pipeline
        # Note: s2d -c - means "decompress from stdin to stdout"
        # .Trace[] extracts each element from the Trace array
        # Full paths to all binaries and explicit PATH for subprocess
        src = (
            f"/usr/local/bin/s2d -c {filepath} | /usr/bin/jq -c '.Trace[]'"
        )

        # Send SQL and data together: SQL as query param, data as POST body
        msg_cmd = f"{src} | curl -sS '{_curl_insert(_sql_insert_messages())}&query={quote(_sql_insert_messages())}' --data-binary @-"
        sub_cmd = f"{src} | curl -sS '{_curl_insert(_sql_insert_subcalls())}&query={quote(_sql_insert_subcalls())}' --data-binary @-"

        try:
            for cmd_name, cmd in [("messages", msg_cmd), ("subcalls", sub_cmd)]:
                print(f"Running {cmd_name} pipeline...")
                proc = subprocess.run(cmd, shell=True, executable=SHELL, capture_output=True, text=True)
                print(f"Subprocess STDERR: {proc.stderr}")
                print(f"Subprocess STDOUT: {proc.stdout}")
                if proc.returncode != 0:
                    raise RuntimeError(f"{cmd_name} pipeline failed with code {proc.returncode}")
                print(f"✓ {cmd_name} pipeline completed")

            # --- SUCCESS ---
            # Delete the running record and insert a success record (for ReplacingMergeTree compatibility)
            c.command(f"DELETE FROM filecoin.loaded_keys WHERE key = '{esc(key)}' AND etag = '{esc(etag)}' AND status = 'running'")
            c.command(f"""
                INSERT INTO filecoin.loaded_keys (key, etag, size_bytes, last_modified, status, attempts, last_error, first_seen, updated_at, processed_at)
                VALUES ('{esc(key)}', '{esc(etag)}', {size}, parseDateTimeBestEffort('{esc(lm)}'), 'success', 
                        (SELECT max(attempts) FROM filecoin.loaded_keys WHERE key = '{esc(key)}'), 
                        '', now(), now(), now())
            """)
            return f"ok {key}"

        except Exception as e:
            # --- FAIL (record error but keep eligibility for retry next run) ---
            err = esc(str(e))[:2048]
            # Delete the running record and insert a failed record
            c.command(f"DELETE FROM filecoin.loaded_keys WHERE key = '{esc(key)}' AND etag = '{esc(etag)}' AND status = 'running'")
            c.command(f"""
                INSERT INTO filecoin.loaded_keys (key, etag, size_bytes, last_modified, status, attempts, last_error, first_seen, updated_at, processed_at)
                VALUES ('{esc(key)}', '{esc(etag)}', {size}, parseDateTimeBestEffort('{esc(lm)}'), 'failed',
                        (SELECT max(attempts) FROM filecoin.loaded_keys WHERE key = '{esc(key)}'),
                        '{err}', now(), now(), NULL)
            """)
            raise

    @task
    def dq_checks():
        c = ch()
        result = c.query("""
            SELECT count() FROM filecoin.messages
            WHERE msg_cid = '' OR to_addr = '' OR from_addr = ''
        """)
        n = result.result_rows[0][0] if result.result_rows else 0
        if n > 0:
            raise RuntimeError(f"DQ failed: {n} bad rows")
        print(f"✓ Data quality check passed: {n} bad rows found")

    init = init_schema()
    keys = list_keys()
    results = ingest_one.expand(obj=keys)
    dq = dq_checks()

    init >> keys >> results >> dq
