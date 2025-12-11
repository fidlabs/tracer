#!/usr/bin/env python3
"""
Download Filecoin traces from the public endpoint:

    https://traces-filecoin.beryx.io/traces/traces_XXXXXXXXXXXX.json.s2

Given a start and end (inclusive) as 12-digit IDs.

Features:
- Sequential one-at-a-time downloads (friendly to tight rate limits)
- Handles 429 and 5xx with Retry-After + exponential backoff
- Skips existing files (resumable)
- Logs missing / failed IDs and writes them to text files
- Progress output on stdout

Usage examples:

    python download_traces.py --start 000000000000 --end 000000000999
    python download_traces.py --start 000000100000 --end 000000101000 --out traces

"""

import argparse
import os
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://traces-filecoin.beryx.io/traces/traces_{id}.json.s2"
DEFAULT_OUT_DIR = "traces"

# HTTP status codes considered transient / rate-limited
RETRY_STATUS = {429, 500, 502, 503, 504}

def parse_id_12(s: str) -> int:
    """Parse a 12-digit string or integer-like into an int, validating range."""
    s = str(s).strip()
    if not s.isdigit():
        raise ValueError(f"ID must be all digits: {s!r}")
    if len(s) > 12:
        raise ValueError(f"ID must be <= 12 digits: {s!r}")
    n = int(s)
    if n < 0:
        raise ValueError("ID must be non-negative")
    return n


def format_id_12(n: int) -> str:
    return f"{n:012d}"


def sleep_with_message(seconds: float, reason: str):
    seconds = max(0.0, seconds)
    if seconds <= 0:
        return
    print(f"  [WAIT] {seconds:.1f}s due to {reason}", flush=True)
    time.sleep(seconds)


def download_one(
    session: requests.Session,
    out_dir: Path,
    id_num: int,
    max_retries: int = 8,
    base_backoff: float = 1.0,
    timeout: float = 30.0,
) -> str:
    """
    Download a single trace file.

    Returns a status string:
      - "ok"       : downloaded (or already existed)
      - "missing"  : 404 from server
      - "failed"   : retry limit exceeded or non-retriable error
    """
    id_str = format_id_12(id_num)
    url = BASE_URL.format(id=id_str)
    filename = f"traces_{id_str}.json.s2"
    dest = out_dir / filename

    # Skip if file already exists
    if dest.exists():
        return "ok"

    attempt = 0
    backoff = base_backoff

    while attempt < max_retries:
        attempt += 1
        try:
            resp = session.get(url, timeout=timeout)
        except requests.RequestException as e:
            # Network / DNS / etc. -> backoff and retry
            print(f"  [ERR ] id={id_str} attempt={attempt}/{max_retries} network error: {e}", flush=True)
            if attempt >= max_retries:
                return "failed"
            sleep_with_message(backoff, "network error")
            backoff *= 2
            continue

        # Successful response
        if resp.status_code == 200:
            try:
                with open(dest, "wb") as f:
                    f.write(resp.content)
            except OSError as e:
                print(f"  [ERR ] id={id_str} write failed: {e}", flush=True)
                return "failed"
            return "ok"

        # Not found: treat as missing and don't retry
        if resp.status_code == 404:
            print(f"  [MISS] id={id_str} 404 Not Found", flush=True)
            return "missing"

        # Rate limit / transient server error
        if resp.status_code in RETRY_STATUS:
            ra = resp.headers.get("Retry-After")
            wait = backoff
            if ra:
                try:
                    wait = max(wait, float(ra))
                except ValueError:
                    # Retry-After as HTTP-date -> ignore and just use backoff
                    pass
            print(f"  [RETRY] id={id_str} status={resp.status_code} attempt={attempt}/{max_retries}", flush=True)
            if attempt >= max_retries:
                return "failed"
            sleep_with_message(wait, f"HTTP {resp.status_code}")
            backoff *= 2
            continue

        # Other non-success, non-retryable status
        print(f"  [FAIL] id={id_str} status={resp.status_code} (no retry)", flush=True)
        return "failed"

    return "failed"


def main():
    parser = argparse.ArgumentParser(description="Download Filecoin traces from public endpoint (rate-limit friendly).")
    parser.add_argument("--start", required=True, help="Start ID (12-digit string or int), inclusive")
    parser.add_argument("--end", required=True, help="End ID (12-digit string or int), inclusive")
    parser.add_argument("--max-retries", type=int, default=8, help="Max retries per file on transient errors")
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout (seconds)")
    parser.add_argument("--base-backoff", type=float, default=1.0, help="Initial backoff (seconds) for retries")
    parser.add_argument("--failohfail", type=bool, default=False, help="Fail as soon as we encounter a missing entry")
    args = parser.parse_args()

    try:
        start_id = parse_id_12(args.start)
        end_id = parse_id_12(args.end)
    except ValueError as e:
        print(f"Invalid ID: {e}", file=sys.stderr)
        sys.exit(1)

    if end_id < start_id:
        print("End ID must be >= start ID", file=sys.stderr)
        sys.exit(1)

    total = end_id - start_id + 1
    missing_ids = []
    failed_ids = []
    ok_count = 0
    last_success = None

    session = requests.Session()
    session.headers.update({"User-Agent": "filecoin-trace-downloader/1.0"})

    start_id_str = format_id_12(start_id)
    print(f"[INFO] Downloading IDs {start_id_str} .. {format_id_12(end_id)} (total {total})")

    out_path = start_id_str[0:4] + "/" + start_id_str[4:8]
    out_dir = Path(out_path).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Now saving files under: {out_dir}")

    try:
        for idx, n in enumerate(range(start_id, end_id + 1), start=1):
            id_str = format_id_12(n)
            progress = f"{idx}/{total} ({idx * 100.0 / total:5.1f}%)"
            print(f"\n[FILE] {progress} id={id_str}", flush=True)

            # Every 10,000 files has a new output bucket
            if n % 10000 == 0:
                out_path = id_str[0:4] + "/" + id_str[4:8]
                out_dir = Path(out_path).resolve()
                out_dir.mkdir(parents=True, exist_ok=True)
                print(f"[INFO] Now saving files under: {out_dir}")

            status = download_one(
                session=session,
                out_dir=out_dir,
                id_num=n,
                max_retries=args.max_retries,
                base_backoff=args.base_backoff,
                timeout=args.timeout,
            )

            if status == "ok":
                ok_count += 1
                last_success = id_str
                print(f"  [OK  ] id={id_str}", flush=True)
            elif status == "missing":
                if args.failohfail:
                    raise FileNotFoundError(f"Trace {id_str} not found") 
                else:
                    missing_ids.append(n)
            else:
                failed_ids.append(n)
                print(f"  [FAIL] id={id_str} (see logs above)", flush=True)

    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user. Writing partial results…", file=sys.stderr)
        raise
    except FileNotFoundError:
        print("\n[INFO] 404 encountered. Aborting.", file=sys.stderr)

    # Write missing / failed lists
    if missing_ids:
        missing_path = out_dir / "missing_ids.txt"
        with open(missing_path, "w", encoding="utf-8") as f:
            for n in missing_ids:
                f.write(format_id_12(n) + "\n")
        print(f"[INFO] Missing IDs: {len(missing_ids)} (written to {missing_path})")

    if failed_ids:
        failed_path = out_dir / "failed_ids.txt"
        with open(failed_path, "w", encoding="utf-8") as f:
            for n in failed_ids:
                f.write(format_id_12(n) + "\n")
        print(f"[INFO] Failed IDs: {len(failed_ids)} (written to {failed_path})")

    if last_success:
        latest_path = "./cronstate/latest_tipset.txt"
        with open(latest_path, "w", encoding="utf-8") as f:
            f.write(last_success)
            print(f"[INFO] Last successful download {last_success} stashed")

    print(f"\n[SUMMARY] OK={ok_count}  MISSING={len(missing_ids)}  FAILED={len(failed_ids)}  TOTAL={total}")


if __name__ == "__main__":
    main()

