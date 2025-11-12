from __future__ import annotations
import os, shlex, subprocess
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
import subprocess
import boto3
import clickhouse_connect
import json
import base64
from web3 import Web3
from web3.contract import Contract
from filecoin_address import  encode, Address, CoinType
from cbor2 import CBORTag, loads
from multiformats import CID

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
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))  # Number of files to process per run
START_FROM = int(os.getenv("START_FROM", "5247857"))    # Starting file number (1-based)
MAX_MISSING_WAIT = int(os.getenv("MAX_MISSING_WAIT", "10"))  # Max consecutive missing files before stopping

SHELL = "bash"

def find_actor_name(actor_address_id):
    actorName = ""
    if actor_address_id == "f06":
        actorName = "verifiedregistry"
    if actor_address_id == "f07":
        actorName = "datacap"
    if actor_address_id == "f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja":
        actorName = "evm"
    return actorName

def find_subcall(subcalls, matchers=None):
    """
    Find the first subcall element that matches any destination/method combination.

    Args:
        subcalls (list): List of subcall objects
        matchers (list): List of arrays, where each array has [destination, method1, method2, ...]
                        First element is the destination address, rest are valid methods
        
    Returns:
        dict or None: First subcall object that matches the condition, or None if no match
    """
    if subcalls is None or matchers is None:
        return None
        
    for subcall in subcalls:
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if this subcall matches any of the matchers
        for matcher in matchers:
            if len(matcher) < 2:
                continue
            
            destination = matcher[0]
            methods = matcher[1:]  # Rest of the array are the methods
            
            # Check if destination matches and method is in the list of methods
            if msg_to == destination and msg_method in methods:
                return subcall
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall(subcall['Subcalls'], matchers)
            if result is not None:
                return result
    
    return None

def strip_until_sentinel(data: bytes) -> bytes:
    """
    Removes leading bytes until the first 0x00 byte is reached.
    
    Args:
        data: The input byte array.
        
    Returns:
        The byte array starting from the first 0x00 (inclusive).
        Returns empty bytes if 0x00 is not found.
    """
    index = data.find(b'\x00')  # find first occurrence of 0x00
    if index == -1:
        return b''  # no sentinel found
    return data[index:]  # include the 0x00
    
def extract_dag_cid(obj):
    """
    Handles CBOR Tag(42, ...) or raw bytes representing a DAG-CBOR link.
    Returns a base32 CID string if possible, otherwise a hex fallback.
    """
    if isinstance(obj, CBORTag) and obj.tag == 42:
        data = obj.value
    elif isinstance(obj, bytes):
        data = obj
    else:
        return None

    data = strip_until_sentinel(data)
    # DAG-CBOR links usually start with 0x00 sentinel
    if data and data[0] == 0x00:
        cid_bytes = data[1:]  # remove sentinel
        try:
            cid = CID.decode(cid_bytes)
            return cid.encode("base32")
        except Exception:
            return cid_bytes.hex()  # fallback
    else:
        # Could be nonstandard; fallback
        return data.hex()
    
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
                        trace_obj = json.loads(line)
                        # matchers format: [[destination, method1, method2, ...], ...]
                        subcall = find_subcall(
                            subcalls=trace_obj['ExecutionTrace']['Subcalls'], 
                            matchers=[["f06", 2, 4, 9, 3916220144], ["f07", 3621052141, 80475954], ["f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja",3844450837]]
                            # matchers=[["f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja",3844450837]]
                        )
                        
                        if subcall:
                            msg = subcall['Msg']
                            aux = []
                            if msg['To'] == "f07" and msg['Method'] == 3621052141 and msg['From'] == "f05":
                                verifregSubcall = find_subcall(subcalls=subcall['Subcalls'], matchers=[["f06", 3726118371]])
                                if verifregSubcall:
                                    if verifregSubcall['Msg']['From']=='f07':
                                        aux= verifregSubcall['MsgRct']

                            print(f"from: {msg['From']}, to: {msg['To']}, method: {msg['Method']}, params: {msg['Params']}")
                            matches.append({"msg": msg, "aux": aux})
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
            "/opt/airflow/plugins/goExecutables/decode_params",
            find_actor_name(obj['msg']['To']),
            str(obj['msg']['Method']),
            obj['msg']['Params'],
            "27"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            decodedParams = json.loads(result.stdout.strip())
            return {"msg": obj['msg'], "aux": obj["aux"], "decodedParams": decodedParams}
        except subprocess.CalledProcessError as e:
            print(f"Failed to decode parameters for {obj}")
            print(f"Command: {' '.join(cmd)}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")
            raise

    @task
    def output_results(decodedResults: dict) -> None:
        batches = { "allocations": [] }
        for obj in decodedResults:
            # allocation from the datacap holder
            if obj['msg']['To'] == "f07" and obj['msg']['Method'] == 3621052141 and obj['msg']['From'] == "f05":
                decodedReceipt = loads(base64.b64decode(obj['aux']['Return']))
                decodedParams = loads(base64.b64decode(obj['decodedParams']['OperatorData']))
                #decodedParams[0] is array of allocations
                allocations = decodedParams[0]
                for i in range(len(allocations)):
                    allocation = allocations[i]
                    allocationId = decodedReceipt[2][i]  # array of allocation IDs
                    batches['allocations'].append(
                        {
                            'id': allocationId,
                            'clientId': int(obj['decodedParams']['From'][2:]),  # strip 'f0' prefix
                            'providerId': allocation[0],
                            'pieceCid': extract_dag_cid(allocation[1]),
                            'pieceSize': allocation[2],
                            'termMin': allocation[3],
                            'termMax': allocation[4],
                            'expiration': allocation[5]
                        }
                    )

            # allocation not from the datacap holder
            if obj['msg']['To'] == "f07" and obj['msg']['Method'] == 80475954:
                print(f"allocation not from datacap holder matched for message")

            # claim 
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 9:
                print(f"claim matched for message")

            # create verifier (f080 multisig)
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 2:
                print(f"create verifier matched for message")

            # create client meta-allocator
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 3916220144:
                print(f"create client meta-allocator matched for message")

            # create client 
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 4:
                print(f"create client matched for message")
    
            # meta-allocator instance
            if obj['msg']['To'] == "f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja" and obj['msg']['Method'] == 3844450837:
                print(f"meta-allocator matched for message")
                hexEthTxInput = '0x' + base64.b64decode(obj['decodedParams']).hex()
                with open('/opt/airflow/plugins/abis/meta-allocator.json', 'r') as f:
                    abi = json.load(f)
            
                w3 = Web3()
                contract = w3.eth.contract(address=Web3.to_checksum_address('0x984376abd1ff5518b6ae9d065c40696ae916dc88'), abi=abi)
                decodedContractFunction = contract.decode_function_input(hexEthTxInput)
                functionName = decodedContractFunction[0].fn_name
                functionParams = decodedContractFunction[1]

                if functionName == "addVerifiedClient":
                    address = Address(functionParams.get('clientAddress'), CoinType.MAIN)
                    print(encode('f', address))
                    print(functionParams.get('amount'))
                    
        print(batches)

    keys = list_keys()
    results = process_trace.expand(obj=keys) 
    flattened = flatten_results(results)
    decoded_results = decode_parameters.expand(obj=flattened)
    output = output_results(decodedResults=decoded_results)
    keys >> results >> flattened >> decoded_results >> output