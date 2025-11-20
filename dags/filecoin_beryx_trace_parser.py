from __future__ import annotations
import os, shlex, subprocess
from datetime import datetime, timezone
from airflow import DAG
from airflow.decorators import task
import subprocess
import clickhouse_connect
import json
import base64
from web3 import Web3
from web3.contract import Contract
from filecoin_address import  encode, Address, CoinType
from cbor2 import CBORTag, loads
from multiformats import CID
import psycopg
import http.client
from urllib.parse import urlparse

CH_HTTP = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))

# Processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "3"))  # Number of files to process per run
START_FROM = int(os.getenv("START_FROM", "5239937"))    # Starting file number (1-based)
MAX_MISSING_WAIT = int(os.getenv("MAX_MISSING_WAIT", "10"))  # Max consecutive missing files before stopping

SHELL = "bash"

def url_is_valid_stdlib(url):
    try:
        parsed = urlparse(url)
        conn = http.client.HTTPConnection(parsed.netloc, timeout=5)
        conn.request("HEAD", parsed.path or "/")
        response = conn.getresponse()
        return response.status < 400
    except Exception:
        return False
    
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

def find_subcall_and_parent(subcalls, parent=None, matchers=None):
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
        return None, None
        
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
                return subcall, parent
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall_and_parent(subcall['Subcalls'], subcall, matchers)
            if result != (None, None):
                return result
    
    return None, None

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
    dag_id="filecoin_beryx_trace_parser",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","beryx","s2","clickhouse","exactly-once-ish"]
) as dag:

    @task
    def list_keys() -> list[dict]:
        """Generate sequential file keys based on deterministic naming pattern.
        
        Files are named: traces_000000000001.json.s2, traces_000000000002.json.s2, etc.
        This function generates BATCH_SIZE sequential keys starting from START_FROM.
        This is a test implementation and it will always process the same files on each run.
        """
       
        keys = []
        missing_count = 0
        current_num = START_FROM
        
        print(f"Starting sequential processing: batch_size={BATCH_SIZE}, start_from={START_FROM}")
        print(f"Looking for files: traces_XXXXXXXXXXXX.json.s2 format")
        
        # Process BATCH_SIZE files sequentially
        for i in range(BATCH_SIZE):
            # Generate the deterministic filename
            filename = f"traces_{current_num:012d}.json.s2"
            key = f"https://traces-filecoin.beryx.io/traces/{filename}"
            
            try:
                # Check if the file exists by trying to get its metadata
                url_is_valid = url_is_valid_stdlib(key)
                
                if url_is_valid is True:
                    keys.append({
                        "key": key,
                        "sequence_number": current_num
                    })
                    
                    print(f"✓ Found {filename} to process.")
                    missing_count = 0  # Reset missing counter
                else:
                    raise Exception("File does not exist")         
                    
            except Exception as e:
                print(f"Error checking {filename}: {str(e)}")
                missing_count += 1
                if missing_count >= MAX_MISSING_WAIT:
                    print(f"Stopping: {missing_count} consecutive missing files (max: {MAX_MISSING_WAIT})")
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
        height = obj["sequence_number"]

        # build common source pipeline
        # Note: s2d -c - means "decompress from stdin to stdout"
        # .Trace[] extracts each element from the Trace array
        # Full paths to all binaries and explicit PATH for subprocess
        src = (
            f" curl {shlex.quote(key)} | /usr/local/bin/s2d -c - | /usr/bin/jq -c '.Trace[]'"
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
                        parent = None

                        subcall = find_subcall(
                            subcalls=trace_obj['ExecutionTrace']['Subcalls'], 
                            matchers=[["f06", 2, 4, 3916220144], ["f07", 3621052141, 80475954 ]]
                            # matchers=[["f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja",3844450837]]
                        )
                        
                        if subcall is None:
                            subcall, parent = find_subcall_and_parent(
                                subcalls=trace_obj['ExecutionTrace']['Subcalls'],
                                parent= trace_obj['ExecutionTrace']['Msg'],
                                matchers=[["f06", 9]]
                            )

                        if subcall:
                            msg = subcall['Msg']
                            verifRegUnivHookRct = []
                            if (msg['To'] == "f07" and msg['Method'] == 3621052141 and msg['From'] == "f05") or (msg['To'] == "f07" and msg['Method'] == 80475954):
                                verifregSubcall = find_subcall(subcalls=subcall['Subcalls'], matchers=[["f06", 3726118371]])
                                if verifregSubcall:
                                    if verifregSubcall['Msg']['From']=='f07':
                                        verifRegUnivHookRct= verifregSubcall['MsgRct']

                            print(f"from: {msg['From']}, to: {msg['To']}, method: {msg['Method']}, params: {msg['Params']}")
                            matches.append({
                                "msg": msg, 
                                "verifRegUnivHookRct": verifRegUnivHookRct, 
                                "parent": parent, 
                                "baseMsg": {
                                    "To": trace_obj['Msg']['To'],
                                    "From": trace_obj['Msg']['From'],
                                    "Version": trace_obj['Msg']['Version'],
                                    "Method": trace_obj['Msg']['Method'],
                                    "Nonce": trace_obj['Msg']['Nonce'],
                                    "MsgCid": trace_obj['MsgCid']["/"]
                                    }, 
                                "height": height
                            })
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
    
    @task
    def decode_parameters(messagesToDecode: dict) -> str:
        """
        Decode parameters using the Go executable with dynamic inputs.
        
        Args:
            obj: Message object with 'To' (actor CID), 'Method', and 'Params'
        
        Returns:
            Decoded parameters as JSON string
        """
        decodedResults = []
        for obj in messagesToDecode:
            try:
                decodeParametersCmd = [
                    "/opt/airflow/plugins/goExecutables/decode_params",
                    find_actor_name(obj['msg']['To']),
                    str(obj['msg']['Method']),
                    obj['msg']['Params'],
                    "27"
                ]
                resultDecodeParametersCmd = subprocess.run(decodeParametersCmd, capture_output=True, text=True, check=True)
                decodedParams = json.loads(resultDecodeParametersCmd.stdout.strip())
            except subprocess.CalledProcessError as e:
                print(f"Failed to decode parameters for {obj}")
                print(f"Command: {' '.join(decodeParametersCmd)}")
                print(f"stdout: {e.stdout}")
                print(f"stderr: {e.stderr}")
                raise

            decodedParamsParent = None

            if obj['parent'] and 'Method' in obj['parent'] and 'Params' in obj['parent'] and (obj['parent']['Method'] == 35 or obj['parent']['Method'] == 34):
                try:
                    decodeParametersCmdParent = [
                        "/opt/airflow/plugins/goExecutables/decode_params",
                        "storageminer",
                        str(obj['parent']['Method']),
                        obj['parent']['Params'],
                        "27"
                    ]
                    resultDecodeParametersCmdParent = subprocess.run(decodeParametersCmdParent, capture_output=True, text=True, check=True)
                    decodedParamsParent = json.loads(resultDecodeParametersCmdParent.stdout.strip())
                except subprocess.CalledProcessError as e:
                    print(f"Failed to decode parent parameters for {obj}")
                    print(f"Command: {' '.join(decodeParametersCmdParent)}")
                    print(f"stdout: {e.stdout}")
                    print(f"stderr: {e.stderr}")
                    raise

            decodedResults.append({
                "msg": obj['msg'], 
                "baseMsg": obj["baseMsg"], 
                "verifRegUnivHookRct": obj["verifRegUnivHookRct"], 
                "parent": obj["parent"], 
                "decodedParams": decodedParams, 
                "decodedParamsParent": decodedParamsParent, 
                "height": obj["height"]
            })
        print(decodedResults)
        return decodedResults

    @task
    def output_results(decodedResults: dict) -> None:
        print(decodedResults)
        batches = { "allocations": [], "claims": [], "verifierAllowances": [], "clientAllowances": [] }
        for obj in decodedResults:
            # allocation
            if (obj['msg']['To'] == "f07" and obj['msg']['Method'] == 3621052141 and obj['msg']['From'] == "f05") or (obj['msg']['To'] == "f07" and obj['msg']['Method'] == 80475954):
                decodedReceipt = loads(base64.b64decode(obj['verifRegUnivHookRct']['Return']))
                decodedParams = loads(base64.b64decode(obj['decodedParams']['OperatorData']))
                clientId = ''
                # Determine clientId based on method
                # 3621052141 allocation from the datacap holder
                # 80475954 allocation not from the datacap holder
                if obj['msg']['Method'] == 3621052141: clientId = obj['decodedParams']['From']
                if obj['msg']['Method'] == 80475954: clientId = obj['msg']['From']

                #decodedParams[0] is array of allocations
                allocations = decodedParams[0]
                for i in range(len(allocations)):
                    allocation = allocations[i]
                    allocationId = decodedReceipt[2][i]  # array of allocation IDs
                    batches['allocations'].append(
                        {
                            'id': allocationId,
                            'clientId': int(clientId[2:]),  # strip 'f0' prefix
                            'providerId': allocation[0],
                            'pieceCid': extract_dag_cid(allocation[1]),
                            'pieceSize': allocation[2],
                            'termMin': allocation[3],
                            'termMax': allocation[4],
                            'expiration': allocation[5]
                        }
                    )

            # claim 
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 9:
                sectors = None
                if obj['parent'] and 'Method' in obj['parent'] and obj['parent']['Method'] == 34:
                    sectors = obj['decodedParamsParent']['SectorActivations']
                if obj['parent'] and 'Method' in obj['parent'] and obj['parent']['Method'] == 35:
                    sectors = obj['decodedParamsParent']['SectorUpdates']

                dealIds = {}
                if sectors:
                    for sector in sectors:
                        sectorNumber = None
                        if obj['parent']['Method'] == 34:
                            sectorNumber =sector['SectorNumber']
                        if obj['parent']['Method'] == 35:
                            sectorNumber = sector['Sector']

                        if sector['Pieces'] is not None:    
                            for piece in sector['Pieces']:
                                if piece['Notify'] is not None:
                                    for notifyItem in piece['Notify']:
                                        if notifyItem['Address'] == 'f05':
                                            dealIds[f"{piece['VerifiedAllocationKey']['Client']}_{piece['VerifiedAllocationKey']['ID']}_{sectorNumber}"] = loads(base64.b64decode(notifyItem['Payload']))

                for sector in obj['decodedParams']['Sectors']:
                    if sector['Claims'] is not None:
                        for claim in sector['Claims']:
                            key = f"{claim['Client']}_{claim['AllocationId']}_{sector['Sector']}"
                            dealId = dealIds.get(key, None)
                            batches['claims'].append(
                                {
                                    'id': claim['AllocationId'],
                                    'clientId': claim['Client'],
                                    'providerId': int(obj['msg']['From'][2:]),
                                    'sector': sector['Sector'],
                                    'dealId': dealId,
                                    'sectorExpiry': sector['SectorExpiry']
                                }
                            )

            # create verifier (f080 multisig)
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 2:
                batches['verifierAllowances'].append(
                    {
                        "verifier": obj['decodedParams']['Address'],
                        "dcSource": obj['msg']['From'],
                        "allowance": obj['decodedParams']['Allowance'],
                        "msgCid": obj['baseMsg']['MsgCid'],
                        "height": obj['height']
                    }
                )

            # create client meta-allocator
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 3916220144:
                batches['clientAllowances'].append(
                    {
                        "virtualVerifier": obj['baseMsg']['To'],
                        "client": obj['decodedParams']['Address'], 
                        "allowance": obj['decodedParams']['Allowance'], 
                        "verifier": obj['msg']['From'],
                        "height": obj['height'],
                        "msgCid": obj['baseMsg']['MsgCid']
                    }
                )

            # create client 
            if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 4:
                batches['clientAllowances'].append(
                    {
                        "virtualVerifier": obj['msg']['From'],
                        "client": obj['decodedParams']['Address'], 
                        "allowance": obj['decodedParams']['Allowance'], 
                        "verifier": obj['msg']['From'],
                        "height": obj['height'],
                        "msgCid": obj['baseMsg']['MsgCid']
                    }
                )
    
            # meta-allocator instance
            # if obj['msg']['To'] == "f410ftbbxnk6r75krrnvotudfyqdjnlurnxei735ruja" and obj['msg']['Method'] == 3844450837:
            #     print(f"meta-allocator matched for message")
            #     hexEthTxInput = '0x' + base64.b64decode(obj['decodedParams']).hex()
            #     with open('/opt/airflow/plugins/abis/meta-allocator.json', 'r') as f:
            #         abi = json.load(f)
            
            #     w3 = Web3()
            #     contract = w3.eth.contract(address=Web3.to_checksum_address('0x984376abd1ff5518b6ae9d065c40696ae916dc88'), abi=abi)
            #     decodedContractFunction = contract.decode_function_input(hexEthTxInput)
            #     functionName = decodedContractFunction[0].fn_name
            #     functionParams = decodedContractFunction[1]

            #     if functionName == "addVerifiedClient":
            #         address = Address(functionParams.get('clientAddress'), CoinType.MAIN)
            #         print(encode('f', address))
            #         print(functionParams.get('amount'))

        with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
            with conn.cursor() as cur:
                # insert allocations
                if batches['allocations']:
                    cur.executemany(
                        "INSERT INTO public.allocations (\"allocationId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"expiration\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (\"allocationId\") DO NOTHING;",
                        [
                            (
                                alloc['id'],
                                alloc['clientId'],
                                alloc['providerId'],
                                alloc['pieceCid'],
                                alloc['pieceSize'],
                                alloc['termMin'],
                                alloc['termMax'],
                                alloc['expiration'],
                            )
                            for alloc in batches['allocations']
                        ]
                    )

                # insert claims
                # Build array of claim ids from batches['claims']
                claim_ids = [claim['id'] for claim in batches['claims']]
                allocations_dict = {}
                if claim_ids:
                    # Select existing allocations from the table
                    cur.execute(
                        "SELECT \"allocationId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"expiration\" FROM public.allocations WHERE \"allocationId\" = ANY(%s);",
                        (claim_ids,)
                    )
                    rows = cur.fetchall()
                    # Build dictionary with id as key and object as value
                    for row in rows:
                        allocations_dict[row[0]] = {
                            'pieceCid': row[3],
                            'pieceSize': row[4],
                            'termMin': row[5],
                            'termMax': row[6],
                        }
                
                if batches['claims']:
                    for claim in batches['claims']:
                        allocationData = {'pieceCid': None, 'pieceSize': None, 'termMin': None, 'termMax': None}

                        alloc = allocations_dict.get(claim['id'], None)
                        if alloc:
                            allocationData = alloc
                        else:
                            print(f"Warning: allocationId {claim['id']} not found in allocations table.")
                        
                        # Update claim with allocation data
                        claim['pieceCid'] = allocationData['pieceCid']
                        claim['pieceSize'] = allocationData['pieceSize']
                        claim['termMin'] = allocationData['termMin']
                        claim['termMax'] = allocationData['termMax']

                    cur.executemany(
                        "INSERT INTO public.deals (\"claimId\", \"clientId\", \"providerId\", \"sectorId\", \"dealId\", \"sectorExpiry\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;",
                        [
                            (
                                claim['id'],
                                claim['clientId'],
                                claim['providerId'],
                                claim['sector'],
                                claim['dealId'],
                                claim['sectorExpiry'],
                                claim['pieceCid'],
                                claim['pieceSize'],
                                claim['termMin'],
                                claim['termMax'],
                            )
                            for claim in batches['claims']
                        ]
                    )

                # insert verifier allowances
                if batches['verifierAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verifier_allowance (\"verifierId\", allowance, \"dcSource\", \"msgCid\", \"height\") VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                va['verifier'],
                                va['allowance'],
                                va['dcSource'],
                                va['msgCid'],
                                va['height']
                            )
                            for va in batches['verifierAllowances']
                        ]
                    )

                # insert client allowances
                if batches['clientAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verified_client_allowance (\"verifierId\", \"clientId\", allowance, \"dcSource\", \"isVirtual\", \"msgCid\", \"height\") VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                ca['virtualVerifier'],
                                ca['client'],
                                ca['allowance'],
                                ca['verifier'],
                                ca['virtualVerifier'] != ca['verifier'],
                                ca['msgCid'],
                                ca['height']
                            )
                            for ca in batches['clientAllowances']
                        ]
                    )

    keys = list_keys()
    results = process_trace.expand(obj=keys) 
    decoded_results = decode_parameters.expand(messagesToDecode=results)
    output = output_results.expand(decodedResults=decoded_results)
    keys >> results >> decoded_results >> output