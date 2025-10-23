from __future__ import annotations
import os, shlex, subprocess
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
import subprocess
import boto3
import clickhouse_connect
from urllib.parse import quote

# Configuration from environment variables
R2_ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_BUCKET = os.environ["R2_BUCKET"]
R2_PREFIX = os.getenv("R2_PREFIX", "")
R2_GLOB = os.getenv("R2_GLOB", "*.json.s2")  # e.g. traces_*.json.s2
R2_ENDPOINT = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

CH_HTTP = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))

# Processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))  # Number of files to process per run
START_FROM = int(os.getenv("START_FROM", "4394387"))    # Starting file number (1-based)
MAX_MISSING_WAIT = int(os.getenv("MAX_MISSING_WAIT", "10"))  # Max consecutive missing files before stopping

SHELL = "bash"

def find_subcall(subcalls, destination=None, method=None):
    """
    Find the first subcall element that matches the destination address and method.

    Args:
        subcalls (list): List of subcall objects
        destination (str): Target address to find match for
        
    Returns:
        dict or None: First subcall object that matches the condition, or None if no match
    """
    if subcalls is None:
        return None
        
    for subcall in subcalls:
        # Check if this subcall matches the destination
        if subcall['Msg']['To'] == destination and subcall['Msg']['Method'] == method:
            return subcall
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            return find_subcall(subcall['Subcalls'], destination, method)
    
    return None

with DAG(
    dag_id="filecoin_r2_s2_trace_parser",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","r2","s2","clickhouse","exactly-once-ish"]
) as dag:

    @task
    def list_keys() -> list[dict]:
        """Generate sequential file keys based on deterministic naming pattern.
        
        Files are named: traces_000000000001.json.s2, traces_000000000002.json.s2, etc.
        This function generates BATCH_SIZE sequential keys starting from START_FROM.
        This is a test implementation and it will always process the same files on each run.
        """
        s3 = boto3.client(
            "s3",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            endpoint_url=R2_ENDPOINT,
            region_name="auto",
        )
        
        keys = []
        missing_count = 0
        current_num = START_FROM
        
        print(f"Starting sequential processing: batch_size={BATCH_SIZE}, start_from={START_FROM}")
        print(f"Looking for files: traces_XXXXXXXXXXXX.json.s2 format")
        
        # Process BATCH_SIZE files sequentially
        for i in range(BATCH_SIZE):
            # Generate the deterministic filename
            filename = f"traces_{current_num:012d}.json.s2"
            key = f"{R2_PREFIX}traces/{filename}" if R2_PREFIX else f"traces/{filename}"
            
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

        print(f"Completed: found {len(keys)} new files to process, stopped at sequence {current_num}")
        return keys

    @task
    def process_trace(obj: dict) -> list[dict]:
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

        # build common source pipeline
        # Note: s2d -c - means "decompress from stdin to stdout"
        # .Trace[] extracts each element from the Trace array
        # Full paths to all binaries and explicit PATH for subprocess
        src = (
            f"AWS_ACCESS_KEY_ID={shlex.quote(R2_ACCESS_KEY_ID)} "
            f"AWS_SECRET_ACCESS_KEY={shlex.quote(R2_SECRET_ACCESS_KEY)} "
            f"PATH=/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin "
            f"/home/airflow/.local/bin/aws --endpoint-url {shlex.quote(R2_ENDPOINT)} s3 cp "
            f"s3://{R2_BUCKET}/{shlex.quote(key)} - | /usr/local/bin/s2d -c - | /usr/bin/jq -c '.Trace[]'"
        )
        
        # Execute the pipeline and parse the JSON output to extract keys
        try:
            result = subprocess.run(src, shell=True, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            
            matches = []
            print(f"Processing {len(lines)} trace objects...")
            
            for i, line in enumerate(lines):
                if line.strip():  # Skip empty lines
                    try:
                        import json
                        trace_obj = json.loads(line)
                        f06_call = find_subcall(subcalls=trace_obj['ExecutionTrace']['Subcalls'], destination="f06", method=2)
                        
                        if f06_call:
                            msg = f06_call['Msg']
                            print(f"from: {msg['From']}, to: {msg['To']}, method: {msg['Method']}, params: {msg['Params']}")
                            matches.append(msg)
                        else:
                            print(f"No f06 subcall in trace {i+1}")

                    except json.JSONDecodeError as e:
                        print(f"Failed to parse JSON on line {i+1}: {e}")
                        print(f"Line content: {line}")
            return matches

        except subprocess.CalledProcessError as e:
            print(f"Pipeline failed: {e}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            raise

    # Flatten the results and apply decode_parameters to each message
    @task
    def flatten_results(results_list):
        """Flatten the list of lists into individual message objects"""
        flattened = []
        for result_group in results_list:
            if isinstance(result_group, list):
                flattened.extend(result_group)
            else:
                flattened.append(result_group)
        return flattened
    
    @task
    def decode_parameters(obj: dict) -> str:
        """
        Decode parameters using the Go executable with dynamic inputs.
        
        Args:
            obj: Message object with 'To' (actor CID), 'Method', and 'Params'
        
        Returns:
            Decoded parameters as JSON string
        """
        
        
        cmd = [
            "/opt/airflow/plugins/goExecutables/datacapstats",
            "bafk2bzaceak2iqpfy4hw6xyyrf7c4yfh7pl4copzm7t63mokecsxfcnybxnd2",
            "2",
            obj['Params']
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Failed to decode parameters for {obj}")
            print(f"Command: {' '.join(cmd)}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            raise

    @task
    def output_results(decoded_param: str) -> None:
        print(f"Decoded parameters: {decoded_param}")

    
    
    keys = list_keys()
    results = process_trace.expand(obj=keys) 
    flattened = flatten_results(results)
    decoded_results = decode_parameters.expand(obj=flattened)
    output = output_results.expand(decoded_param=decoded_results)
    keys >> results >> flattened >> decoded_results >> output