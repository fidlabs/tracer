"""File management utilities for trace file enumeration and cursor tracking."""
from pathlib import Path


def key_from_seq(n: int, traces_root: Path) -> Path:
    """
    Generate file path from sequence number.
    
    Bucketed directories of 10,000 files:
      top = n // 100_000_000  -> 0000 for current range
      mid = n // 10_000       -> e.g. 0000, 0553
      file name = traces_<12d>.json.s2
    """
    z = f"{n:012d}"
    top = n // 100_000_000
    mid = n // 10_000
    return traces_root / f"{top:04d}" / f"{mid:04d}" / f"traces_{z}.json.s2"


def etag_for_file(p: Path) -> str:
    """
    Generate etag from file stats.
    
    Cheap versioning: size + mtime_ns; fast and good enough to detect changes.
    """
    st = p.stat()
    return f"{st.st_size}-{st.st_mtime_ns}"


def find_cursor(ch_client, start_seq: int) -> int:
    """
    Query ClickHouse for last successful seq_num.
    
    Returns the highest seq_num with status='success', or start_seq - 1 if none found.
    Uses FINAL to ensure ReplacingMergeTree has merged duplicates.
    """
    result = ch_client.query("SELECT max(seq_num) FROM filecoin.seq_load_state FINAL WHERE status='success'")
    row = result.result_rows[0][0] if result.result_rows else None
    cursor = int(row) if row is not None else (start_seq - 1)
    return cursor


def plan_next_batch(prev: int, traces_root: Path, batch_max: int) -> list[dict]:
    """
    Walk forward from prev+1, check local file exists, stop on first gap.
    
    Return up to batch_max present files with 'etag' (size+mtime).
    Each dict contains: {'seq': int, 'path': str, 'etag': str}
    
    Handles PermissionError gracefully - if we can't check a file due to permissions,
    we treat it as a gap and stop processing.
    """
    out = []
    seq = prev + 1
    
    for i in range(batch_max):
        p = key_from_seq(seq, traces_root)
        try:
            if not p.exists():
                break  # stop at first hole
            etag = etag_for_file(p)
            out.append({"seq": seq, "path": str(p), "etag": etag})
        except PermissionError as e:
            # Can't access file due to permissions - treat as gap and stop
            print(f"⚠ Permission denied checking seq {seq}: {p}")
            print(f"  Error: {e}")
            print(f"  Stopping batch enumeration at this gap")
            break
        seq += 1
    
    return out

