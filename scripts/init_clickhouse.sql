CREATE DATABASE IF NOT EXISTS filecoin;

-- Raw transactions (billions of rows are fine; see notes below)
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

-- Future “special processing” queue (filled later via SQL or MV)
CREATE TABLE IF NOT EXISTS filecoin.special_queue
(
  seq_num    UInt64,
  to_addr    String,
  method     UInt32,
  msg_cid    String,
  params_b64 String CODEC(ZSTD(6)),
  enqueued_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY intDiv(seq_num, 10000000)
ORDER BY (to_addr, method, msg_cid);
