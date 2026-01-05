CREATE DATABASE IF NOT EXISTS filecoin;

CREATE TABLE IF NOT EXISTS filecoin.messages
(
    msg_cid           String,
    msg_version       UInt8,
    to_addr           String,
    from_addr         String,
    nonce             UInt64,
    value_atto        Decimal(38, 0),
    gas_limit         UInt64,
    gas_fee_cap       Decimal(38, 0),
    gas_premium       Decimal(38, 0),
    method            UInt32,
    params_b64        String CODEC(ZSTD(6)),

    rct_exit_code     Int32,
    rct_return_b64    Nullable(String) CODEC(ZSTD(6)),
    rct_gas_used      UInt64,

    base_fee_burn     Decimal(38, 0),
    overestimation_burn Decimal(38, 0),
    miner_penalty     Decimal(38, 0),
    miner_tip         Decimal(38, 0),
    refund            Decimal(38, 0),
    total_cost        Decimal(38, 0),

    trace_duration_ns UInt64,
    trace_error       String,

    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (msg_cid);

CREATE INDEX IF NOT EXISTS idx_to_addr_bf ON filecoin.messages (to_addr) TYPE bloom_filter GRANULARITY 64;
CREATE INDEX IF NOT EXISTS idx_from_addr_bf ON filecoin.messages (from_addr) TYPE bloom_filter GRANULARITY 64;

CREATE TABLE IF NOT EXISTS filecoin.subcalls
(
    parent_cid        String,
    idx               UInt16,
    to_addr           String,
    from_addr         String,
    method            UInt32,
    exit_code         Int32,
    return_b64        Nullable(String) CODEC(ZSTD(6)),
    gas_used          UInt64,
    duration_ns       UInt64,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (parent_cid, idx);

-- Tailored indexes / rollups

CREATE TABLE IF NOT EXISTS filecoin.idx_actor_method_daily
(
    day         Date,
    to_addr     String,
    method      UInt32,
    cnt         UInt64,
    success     UInt64,
    gas_used    UInt64,
    total_cost  Decimal(38, 0)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (to_addr, method, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS filecoin.mv_actor_method_daily
TO filecoin.idx_actor_method_daily
AS
SELECT
    toDate(ingested_at) AS day,
    to_addr,
    method,
    count() AS cnt,
    sum(rct_exit_code = 0) AS success,
    sum(rct_gas_used) AS gas_used,
    sum(total_cost) AS total_cost
FROM filecoin.messages
GROUP BY day, to_addr, method;

CREATE TABLE IF NOT EXISTS filecoin.idx_from_to_daily
(
    day         Date,
    from_addr   String,
    to_addr     String,
    method      UInt32,
    cnt         UInt64,
    value_atto  Decimal(38, 0),
    total_cost  Decimal(38, 0)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (from_addr, to_addr, method, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS filecoin.mv_from_to_daily
TO filecoin.idx_from_to_daily
AS
SELECT
    toDate(ingested_at) AS day,
    from_addr,
    to_addr,
    method,
    count() AS cnt,
    sum(value_atto) AS value_atto,
    sum(total_cost) AS total_cost
FROM filecoin.messages
GROUP BY day, from_addr, to_addr, method;

CREATE TABLE IF NOT EXISTS filecoin.loaded_keys
(
  key           String,
  etag          String,
  size_bytes    UInt64,
  last_modified DateTime,
  status        Enum8('running' = 0, 'success' = 1, 'failed' = 2),
  attempts      UInt16,
  last_error    String,
  first_seen    DateTime DEFAULT now(),
  updated_at    DateTime DEFAULT now(),
  processed_at  Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (key);

-- Raw transactions table for sequential local ingestion
CREATE TABLE IF NOT EXISTS filecoin.transactions_raw
(
  seq_num      UInt64,                         -- file number (e.g., 5539999)
  msg_version  UInt8,
  to_addr      String,
  from_addr    String,
  value_atto   Decimal(38,0),
  method       UInt32,
  params_b64   String CODEC(ZSTD(6)),
  msg_cid      String,
  ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY intDiv(seq_num, 10000000)          -- 10M seqs per partition
ORDER BY (seq_num, msg_cid)
SETTINGS index_granularity = 8192;

CREATE INDEX IF NOT EXISTS bf_to_addr   ON filecoin.transactions_raw (to_addr)   TYPE bloom_filter GRANULARITY 64;
CREATE INDEX IF NOT EXISTS bf_from_addr ON filecoin.transactions_raw (from_addr) TYPE bloom_filter GRANULARITY 64;
CREATE INDEX IF NOT EXISTS bf_msg_cid   ON filecoin.transactions_raw (msg_cid)   TYPE bloom_filter GRANULARITY 64;

-- Per-file load state (exactly-once-ish per (seq_num, etag))
CREATE TABLE IF NOT EXISTS filecoin.seq_load_state
(
  seq_num      UInt64,
  etag         String,
  status       Enum8('running'=0,'success'=1,'failed'=2),
  attempts     UInt16,
  last_error   String,
  updated_at   DateTime DEFAULT now(),
  processed_at Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (seq_num);
