from __future__ import annotations
import os, shlex, subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from airflow import DAG
from airflow.decorators import task

# --- config ---
TRACES_ROOT = Path(os.getenv("TRACES_ROOT", "/data/traces"))  # mounted host dir
CH_HTTP     = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST     = os.getenv("CH_HOST", "clickhouse")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
START_SEQ   = int(os.getenv("START_SEQ", "1"))     # where to begin if no state
BATCH_MAX   = int(os.getenv("BATCH_MAX", "2000"))  # max files per run
SHELL       = "bash"                               # for shell pipelines

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
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

def ch_url(sql: str) -> str:
    return f"{CH_HTTP}/?query={quote(sql)}"

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
    schedule=None,         # trigger ad-hoc, or add cron
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","s2","clickhouse","seq","local"]
) as dag:

    @task
    def find_cursor() -> int:
        c = ch_client()
        row = c.query("SELECT max(seq_num) FROM filecoin.seq_load_state WHERE status='success'").first_item()
        return int(row) if row is not None else (START_SEQ - 1)

    @task
    def plan_next_batch(prev: int) -> list[dict]:
        """
        Walk forward from prev+1, check local file exists, stop on first gap.
        Return up to BATCH_MAX present files with 'etag' (size+mtime).
        """
        out = []
        seq = prev + 1
        for _ in range(BATCH_MAX):
            p = key_from_seq(seq)
            if not p.exists():
                break  # stop at first hole
            etag = etag_for_file(p)
            out.append({"seq": seq, "path": str(p), "etag": etag})
            seq += 1
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

        # escape helper for single-quoted SQL
        def esc(s: str) -> str: return s.replace("\\","\\\\").replace("'","\\'")

        c = ch_client()

        # CLAIM: only if not already success for same (seq, etag)
        claim_sql = f"""
        INSERT INTO filecoin.seq_load_state (seq_num, etag, status, attempts, last_error, updated_at, processed_at)
        SELECT {seq}, '{esc(etag)}', 'running',
               ifNull((SELECT max(attempts) FROM filecoin.seq_load_state WHERE seq_num={seq}),0)+1,
               '', now(), NULL
        WHERE NOT EXISTS (
          SELECT 1 FROM filecoin.seq_load_state WHERE seq_num={seq} AND etag='{esc(etag)}' AND status='success'
        )
        """
        inserted = c.command(claim_sql)
        if inserted == 0:
            return f"skip {seq} (already success)"

        # stream: s2 -> compact JSON lines -> ClickHouse HTTP insert
        src  = f"s2dec < {shlex.quote(path)} | jq -c '.Trace[]'"
        curl = f"curl -sS '{ch_url(insert_sql_for_seq(seq))}' --data-binary @-"

        try:
            proc = subprocess.run(f"{src} | {curl}", shell=True, executable=SHELL)
            if proc.returncode != 0:
                raise RuntimeError(f"pipeline failed for seq {seq}")

            # SUCCESS
            c.command(f"""
              ALTER TABLE filecoin.seq_load_state
              UPDATE status='success', processed_at=now(), updated_at=now(), last_error=''
              WHERE seq_num={seq} AND status='running'
            """)
            return f"ok {seq}"

        except Exception as e:
            err = esc(str(e))[:2048]
            c.command(f"""
              ALTER TABLE filecoin.seq_load_state
              UPDATE status='failed', updated_at=now(), last_error='{err}'
              WHERE seq_num={seq} AND status='running'
            """)
            raise

    @task
    def route_special(_results: list[str]) -> None:
        """
        Stub: later, replace with logic to enqueue to filecoin.special_queue
        or branch to per-actor handlers based on (to_addr, method).
        """
        print("special processing stub: TODO")

    prev = find_cursor()
    batch = plan_next_batch(prev)
    results = ingest_one.expand(obj=batch)
    route = route_special(results)

    prev >> batch >> results >> route
