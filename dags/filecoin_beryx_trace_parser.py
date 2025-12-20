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
from filecoin_address import  encode, Address, CoinType, delegated_from_eth_address
from cbor2 import CBORTag, loads
from multiformats import CID
import psycopg
import http.client
from urllib.parse import urlparse
import requests

LOTUS_API_URL = os.environ["LOTUS_API_URL"]
LOTUS_AUTH_TOKEN = os.environ["LOTUS_AUTH_TOKEN"]

CH_HTTP = os.getenv("CH_HTTP", "http://clickhouse:8123")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))

# Processing configuration
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))  # Number of files to process per run
START_FROM = int(os.getenv("START_FROM", "4784822"))    # Starting file number (1-based)
MAX_MISSING_WAIT = int(os.getenv("MAX_MISSING_WAIT", "10"))  # Max consecutive missing files before stopping


SHELL = "bash"
def eth_address_to_fil_native(address: str) -> str:
    has_prefix = address.lower().startswith('0xff0000000000000000000000')              
    if has_prefix:
        filNativeAddress = "f0" + str(int.from_bytes(bytes.fromhex(address[26:]), byteorder="big", signed=False))
    else:
        filNativeAddress = delegated_from_eth_address(address)
    return filNativeAddress

def address_is_id(address: str | int) -> bool:
    if isinstance(address, int):
        return True
    if isinstance(address, str) and (address.startswith("f0") or address.startswith("t0")):
        return True
    return False

def address_to_id(address: str | int) -> int:
    if isinstance(address, int):
        return address
    if isinstance(address, str) and (address.startswith("f0") or address.startswith("t0")):
        return int(address[2:])
    return 0

def normalize_address_to_id(address: str | int, addressDictionary, addressIdDictionary) -> int:
    result = 0
    if address == 5 or address == "f05":
        return 5, addressDictionary, addressIdDictionary
    
    if isinstance(address, int):
        result = address
    if isinstance(address, str) and address.startswith("f0"):
        result = int(address[2:])

    if result != 0:
        if result in addressIdDictionary:
            addressStr = addressIdDictionary[result]
            if (addressIdDictionary[result] == "" or addressIdDictionary[result] is None):
                addressStr = lotus_api_call(method="Filecoin.StateAccountKey", params=[f"f0{result}", None])
                if not isinstance(addressStr, str) or addressStr.startswith("failed"):
                    addressStr = lotus_api_call(method="Filecoin.StateLookupRobustAddress", params=[f"f0{result}", None])

                if isinstance(addressStr, str) and addressStr.startswith("f"):
                    addressIdDictionary[result] = addressStr
                    with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "update actors set address = %s, \"addressEth\" = %s where \"addressId\" = %s",
                                (addressStr, None, result)
                            )
                    
        else:
            print(f"Not found in addressIdDictionary, making Lotus API call {result}")
            addressStr = lotus_api_call(method="Filecoin.StateAccountKey", params=[f"f0{result}", None])
            if not isinstance(addressStr, str) or addressStr.startswith("failed"):
                    addressStr = lotus_api_call(method="Filecoin.StateLookupRobustAddress", params=[f"f0{result}", None])

            if isinstance(addressStr, str) and addressStr.startswith("f"):
                addressIdDictionary[result] = addressStr
                with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "insert into actors (\"addressId\", address, \"addressEth\") values (%s, %s, %s)",
                            (result, addressStr, None)
                        )
            
        return result, addressDictionary, addressIdDictionary

    if address in addressDictionary:
        result = addressDictionary[address]   

    if result == 0:
        print(f"Native address {result}")
        if isinstance(address, str) and (address.startswith("f1") or address.startswith("f3")):
            lotus_api_call_result = lotus_api_call(method="Filecoin.StateLookupID", params=[address, None])
            if (isinstance(lotus_api_call_result, str) and lotus_api_call_result.startswith("f0")):
                result = int(lotus_api_call_result[2:])
                addressDictionary[address] = result
                with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "insert into actors (\"addressId\", address, \"addressEth\") values (%s, %s, %s)",
                            (result, address, None)
                        )
                
            
    return result, addressDictionary, addressIdDictionary

def lotus_api_call(method: str, params: list):
    print(f"Making Lotus API call: {method} with params: {params}")
    print(f"Using LOTUS_API_URL: {LOTUS_API_URL}")
    print(f"Using LOTUS_AUTH_TOKEN: {LOTUS_AUTH_TOKEN[:10]}...")  # Print only the beginning for security
    headers = {
        'Content-Type': 'application/json',
        'Authorization': LOTUS_AUTH_TOKEN
    }
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }
    
    response = requests.post(LOTUS_API_URL, headers=headers, json=payload)
    response.raise_for_status()
    
    result = response.json()
    return result.get('result')

def url_is_valid_stdlib(url):
    try:
        parsed = urlparse(url)
        conn = http.client.HTTPConnection(parsed.netloc, timeout=5)
        conn.request("HEAD", parsed.path or "/")
        response = conn.getresponse()
        return response.status < 400
    except Exception:
        return False
    
def get_network_version( height: int) -> str:
    """
    Returns the Filecoin network version for a given block height.
    """
    network_upgrades_summary = [
        {"upgrade": 6,    "name": "Kumquat",      "actorsVersion": "v2.2.0",  "epoch": 170000},
        {"upgrade": 7,    "name": "Calico",       "actorsVersion": "v2.3.2",  "epoch": 265200},
        {"upgrade": 8,    "name": "Persian",      "actorsVersion": "v2.3.2",  "epoch": 272400},
        {"upgrade": 9,    "name": "Orange",       "actorsVersion": "v2.3.3",  "epoch": 336458},
        {"upgrade": "9.5","name": "Claus",        "actorsVersion": "v2.3.3",  "epoch": 343200},
        {"upgrade": 10,   "name": "Trust",        "actorsVersion": "v3.0.3",  "epoch": 550321},
        {"upgrade": 11,   "name": "Norwegian",    "actorsVersion": "v3.1.0",  "epoch": 665280},
        {"upgrade": 12,   "name": "Turbo",        "actorsVersion": "v4.0.0",  "epoch": 712320},
        {"upgrade": 13,   "name": "HyperDrive",   "actorsVersion": "v5.0.0",  "epoch": 892800},
        {"upgrade": 14,   "name": "Chocolate",    "actorsVersion": "v6.0.0",  "epoch": 1231620},
        {"upgrade": 15,   "name": "OhSnap",       "actorsVersion": "v7.0.0",  "epoch": 1594680},
        {"upgrade": 16,   "name": "Skyr",         "actorsVersion": "v8.0.0",  "epoch": 1960320},
        {"upgrade": 17,   "name": "Shark",        "actorsVersion": "v9.0.0",  "epoch": 2383680},
        {"upgrade": 18,   "name": "Hygge",        "actorsVersion": "v10.0.0", "epoch": 2683348},
        {"upgrade": 19,   "name": "Lightning",    "actorsVersion": "v11.0.0", "epoch": 2809800},
        {"upgrade": 20,   "name": "Thunder",      "actorsVersion": "v11.0.0", "epoch": 2870280},
        {"upgrade": 21,   "name": "Watermelon",   "actorsVersion": "v12.0.0", "epoch": 3469380},
        {"upgrade": 22,   "name": "Dragon",       "actorsVersion": "v13.0.0", "epoch": 3817920},
        {"upgrade": 23,   "name": "Waffle",       "actorsVersion": "v14.0.0", "epoch": 4154640},
        {"upgrade": 24,   "name": "Tuk Tuk",      "actorsVersion": "v15.0.0", "epoch": 4461240},
        {"upgrade": 25,   "name": "Teep",         "actorsVersion": "v16.0.0", "epoch": 4867320},
        {"upgrade": 26,   "name": "Tock",         "actorsVersion": "v16.0.0", "epoch": 5126520},
        {"upgrade": 27,   "name": "Golden Week",  "actorsVersion": "v17.0.0", "epoch": 5348280} 
    ]
    network_version = 0
    for upgrade in network_upgrades_summary:
        if height >= upgrade["epoch"]:
            network_version = upgrade["upgrade"]
        else:
            break
    return str(network_version)
    
def find_actor_name(actor_address_id):
    actorName = "evm"
    if actor_address_id == "f06":
        actorName = "verifiedregistry"
    if actor_address_id == "f05":
        actorName = "storagemarket"    
    if actor_address_id == "f07":
        actorName = "datacap"
    if actor_address_id == "f04":
        actorName = "storagepower"
    if actor_address_id == "f010":
        actorName = "eam"
    return actorName

def find_subcall (subcalls, destination, method):
    if subcalls is None or destination is None or method is None:
        return None    
    for subcall in subcalls:
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if destination matches and method is in the list of methods
        if msg_to == destination and msg_method == method:
            return subcall
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall(subcall['Subcalls'], destination, method)
            if result is not None:
                return result
    
    return None

def find_subcall_and_parent(subcalls, destination, method,  parent=None):
    if subcalls is None or destination is None or method is None:
        return None    
    for subcall in subcalls:
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if destination matches and method is in the list of methods
        if msg_to == destination and msg_method == method:
            return subcall, parent
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall_and_parent(subcall['Subcalls'], destination, method, subcall)
            if result is not None:
                return result
   
    return None, None

def find_subcall_and_path(subcalls, destination, method, path=[]):
    if subcalls is None or destination is None or method is None:
        return None    
    for subcall in subcalls:
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if destination matches and method is in the list of methods
        if msg_to == destination and msg_method == method:
            return subcall, path
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall_and_path(subcall['Subcalls'], destination, method, path + [{ "to": subcall['Msg']['To'], "from": subcall['Msg']['From'] }])
            if result is not None:
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
                        
                        if height <=3855360:
                            matchers=[["f05", 4], ["f06", 2, 4], ["f04", 8]]
                            matchersWithParents = []
                            matchersWithPaths = []
                            postNV22 = False
                        else:
                            matchers=[["f06", 2, 4, 3916220144], ["f07", 3621052141 ], ["f010", 3]]
                            # matchers=[]
                            if height > 3996816:
                                with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
                                    with conn.cursor() as cur:
                                        rows = cur.execute("select \"address\", \"addressId\" from meta_allocators")
                                        for row in rows:
                                            matchers.append( [row[0], 3844450837] )
                                            matchers.append( [row[1], 3844450837] )
                                        rows = cur.execute("select \"address\", \"addressId\" from client_contracts")
                                        for row in rows:
                                            matchers.append( [row[0], 3844450837] )
                                            matchers.append( [row[1], 3844450837] )

                            matchersWithParents = [["f06", 9]]
                            matchersWithPaths = [["f07", 80475954]]
                            postNV22 = True

                        subcalls = [];

                        for matcher in matchers:
                            if len(matcher) < 2:
                                continue
                            
                            destination = matcher[0]
                            methods = matcher[1:]  # Rest of the array are the methods
                            
                            for method in methods:
                                subcall = find_subcall(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method)
                                if (subcall is not None):
                                    subcalls.append({"subcall": subcall, "parent": None, "path": []})

                        for matcher in matchersWithParents:
                            if len(matcher) < 2:
                                continue
                            
                            destination = matcher[0]
                            methods = matcher[1:]  # Rest of the array are the methods
                            
                            for method in methods:
                                subcall, parent = find_subcall_and_parent(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method, parent=None)
                                if (subcall is not None):
                                    subcalls.append({"subcall": subcall, "parent": parent, "path": []})

                        for matcher in matchersWithPaths:
                            if len(matcher) < 2:
                                continue
                            
                            destination = matcher[0]
                            methods = matcher[1:]  # Rest of the array are the methods
                            
                            for method in methods:
                                subcall, path = find_subcall_and_path(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method, path=[]) 
                                if (subcall is not None):
                                    subcalls.append({"subcall": subcall, "parent": None, "path": path})

                        for match in subcalls:
                            if "subcall" in match:
                                msg = match["subcall"]['Msg']
                                msgRct = match["subcall"]['MsgRct']
                                verifRegUnivHookRct = []
                                if (msg['To'] == "f07" and msg['Method'] == 3621052141 and msg['From'] == "f05") or (msg['To'] == "f07" and msg['Method'] == 80475954):
                                    verifregSubcall = find_subcall(subcalls=match["subcall"]['Subcalls'], destination="f06", method=3726118371)
                                    if verifregSubcall:
                                        if verifregSubcall['Msg']['From']=='f07':
                                            verifRegUnivHookRct= verifregSubcall['MsgRct']

                                print(f"from: {msg['From']}, to: {msg['To']}, method: {msg['Method']}, params: {msg['Params']}")
                                matches.append({
                                    "msg": msg, 
                                    "msgRct": msgRct,
                                    "verifRegUnivHookRct": verifRegUnivHookRct, 
                                    "parent": match["parent"], 
                                    "path": match["path"],
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
                                print(f"No matched subcall in trace {i+1}")

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
                actorName = find_actor_name(obj['msg']['To'])
                if obj['msg']['ParamsCodec'] == 81:
                    decodeParametersCmd = [
                        "/opt/airflow/plugins/goExecutables/decode_params",
                        actorName,
                        str(obj['msg']['Method']),
                        obj['msg']['Params'],
                        get_network_version( obj['height'])
                    ]
                    resultDecodeParametersCmd = subprocess.run(decodeParametersCmd, capture_output=True, text=True, check=True)
                    decodedParams = json.loads(resultDecodeParametersCmd.stdout.strip())
                else: 
                    if obj['msg']['ParamsCodec'] == 85:
                        decodedParams = obj['msg']['Params']
                    else:
                        decodedParams = None
                
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
                        get_network_version( obj['height'])
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
                "msgRct":  obj['msgRct'],
                "baseMsg": obj["baseMsg"], 
                "verifRegUnivHookRct": obj["verifRegUnivHookRct"], 
                "parent": obj["parent"], 
                "path": obj["path"],
                "decodedParams": decodedParams, 
                "decodedParamsParent": decodedParamsParent, 
                "height": obj["height"]
            })
        return decodedResults

    @task
    def output_results(decodedResults: dict) -> None:
        with psycopg.connect("host=postgres port=5432 dbname=filecoin connect_timeout=10 user=airflow password=airflow") as conn:
            with conn.cursor() as cur:
                metaAllocatorAddressDictionary = {}
                clientContractAddressDictionary = {}

                rows = cur.execute("select \"address\", \"addressId\", \"addressEth\" from meta_allocators")
                for row in rows:
                    metaAllocatorAddressDictionary[row[0]] = (row[2])
                    metaAllocatorAddressDictionary[row[1]] = (row[2])

                rows = cur.execute("select \"address\", \"addressId\", \"addressEth\" from client_contracts")
                for row in rows:
                    clientContractAddressDictionary[row[0]] = (row[2])
                    clientContractAddressDictionary[row[1]] = (row[2])

                batches = { "allocations": [], "claims": [], "verifierAllowances": [], "clientAllowances": [], "proposals": [], "sectorActivations": [], "metaAllocators": [], "clientContracts": [] }
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

                        contractImmediateCaller = None
                        if (len(obj['path']) > 1):
                            contractImmediateCaller = obj['path'][-2]['from']
                        else :
                            contractImmediateCaller = obj['msg']['From']

                        print(f"path: {obj['path']}")
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
                                    'expiration': allocation[5],
                                    'contractImmediateCaller': contractImmediateCaller,
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
                                            'pieceCid': claim['Data']['/'],
                                            'pieceSize': claim['Size'],
                                            'providerId': int(obj['msg']['From'][2:]),
                                            'sector': sector['Sector'],
                                            'dealId': dealId,
                                            'sectorExpiry': sector['SectorExpiry'],
                                            'termStart': obj['height'],
                                        }
                                    )

                    # create verifier (f080 multisig)
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 2:
                        batches['verifierAllowances'].append(
                            {
                                "verifierId": obj['decodedParams']['Address'],
                                "allowance": obj['decodedParams']['Allowance'],
                                "msgCid": obj['baseMsg']['MsgCid'],
                                "height": obj['height']
                            }
                        )

                    # create client meta-allocator
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 3916220144:
                        batches['clientAllowances'].append(
                            {
                                "clientId": obj['decodedParams']['Address'], 
                                "allowance": obj['decodedParams']['Allowance'], 
                                "verifierId": obj['msg']['From'],
                                "height": obj['height'],
                                "msgCid": obj['baseMsg']['MsgCid']
                            }
                        )

                    # create client 
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 4:
                        batches['clientAllowances'].append(
                            {
                                "clientId": obj['decodedParams']['Address'], 
                                "allowance": obj['decodedParams']['Allowance'], 
                                "verifierId": obj['msg']['From'],
                                "height": obj['height'],
                                "msgCid": obj['baseMsg']['MsgCid']
                            }
                        )
            
                    # proposal 
                    if obj['msg']['To'] == "f05" and obj['msg']['Method'] == 4:
                        decodedReceipt = loads(base64.b64decode(obj['msgRct']['Return']))
                        proposalIndex = 0
                        for deal in obj['decodedParams']['Deals']:
                            proposal = deal['Proposal']
                            if proposal['VerifiedDeal'] is True:
                                batches['proposals'].append(
                                    {
                                        'dealId': decodedReceipt[0][proposalIndex],
                                        'clientId': proposal['Client'],
                                        'pieceCid': proposal['PieceCID']['/'],
                                        'pieceSize': proposal['PieceSize'],
                                        'providerId': proposal['Provider'],
                                        'label': proposal['Label'],
                                        'verified': proposal['VerifiedDeal'],
                                        'storagePricePerEpoch': proposal['StoragePricePerEpoch'],
                                        'clientCollateral': proposal['ClientCollateral'],
                                        'providerCollateral': proposal['ProviderCollateral'],
                                        'startEpoch': proposal['StartEpoch'],
                                        'endEpoch': proposal['EndEpoch'],
                                    }
                                )
                            proposalIndex += 1

                    # sector activations 
                    if obj['msg']['To'] == "f04" and obj['msg']['Method'] == 8:
                        if obj['decodedParams']['DealIDs'] is not None:
                            for dealId in obj['decodedParams']['DealIDs']:
                                batches['sectorActivations'].append(
                                    {
                                        'dealId': dealId,
                                        'providerId': int(obj['decodedParams']['Miner']),
                                        'activationHeight': obj['height'],
                                        'sectorNumber': int(obj['decodedParams']['Number'])
                                    }
                                )  

                    # sector activations 
                    if obj['msg']['To'] == "f010" and obj['msg']['Method'] == 3 and obj['msgRct']['ExitCode'] == 0 and (obj['msg']['From'] == "f03239905" or obj['msg']['From'] == "f03136590"):
                        decodedReceipt = loads(base64.b64decode(obj['msgRct']['Return']))

                        addressId = decodedReceipt[0]
                        address = Address(decodedReceipt[1], CoinType.MAIN)
                        addressEth = decodedReceipt[2]

                        if (obj['msg']['From'] == "f03239905"):
                            batches['clientContracts'].append(
                                    {
                                        'addressId': addressId,
                                        'address': encode('f', address),
                                        'addressEth': "0x" + addressEth.hex()
                                    }
                                )  

                        if (obj['msg']['From'] == "f03136590"):
                            batches['metaAllocators'].append(
                                    {
                                        'addressId': addressId,
                                        'address': encode('f', address),
                                        'addressEth': "0x" + addressEth.hex()
                                    }
                                )  

                    # meta-allocator instance
                    if obj['msg']['To'] in metaAllocatorAddressDictionary and obj['msg']['Method'] == 3844450837:
                        print(f"meta-allocator matched for message")
                        hexEthTxInput = '0x' + base64.b64decode(obj['decodedParams']).hex()
                        with open('/opt/airflow/plugins/abis/meta-allocator.json', 'r') as f:
                            abi = json.load(f)
                    
                        w3 = Web3()
                        contract = w3.eth.contract(address=Web3.to_checksum_address(metaAllocatorAddressDictionary[obj['msg']['To']]), abi=abi)
                        decodedContractFunction = contract.decode_function_input(hexEthTxInput)
                        functionName = decodedContractFunction[0].fn_name
                        functionParams = decodedContractFunction[1]

                        print(f"meta-allocator function: {functionName}, params: {functionParams}")
                        if functionName == "addAllowance":
                            address = str(functionParams.get('allocator'))
                            filNativeAddress = eth_address_to_fil_native(address)
 
                            batches['verifierAllowances'].append(
                            {
                                "verifierId": filNativeAddress,
                                "allowance": functionParams.get('amount'),
                                "msgCid": obj['baseMsg']['MsgCid'],
                                "height": obj['height']
                            }
                        )
                            
                    # client contract instance
                    if obj['msg']['To'] in clientContractAddressDictionary and obj['msg']['Method'] == 3844450837:
                        print(f"client contract matched for message")
                        hexEthTxInput = '0x' + base64.b64decode(obj['decodedParams']).hex()
                       
                        with open('/opt/airflow/plugins/abis/client-contract.json', 'r') as f:
                            abi = json.load(f)
                    
                        w3 = Web3()
                        contract = w3.eth.contract(address=Web3.to_checksum_address(clientContractAddressDictionary[obj['msg']['To']]), abi=abi)
                        decodedContractFunction = contract.decode_function_input(hexEthTxInput)
                        functionName = decodedContractFunction[0].fn_name
                        functionParams = decodedContractFunction[1]

                        print(f"client contract function: {functionName}, params: {functionParams}")
                        if functionName == "increaseAllowance":
                            address = str(functionParams.get('client'))
                            filNativeAddress = eth_address_to_fil_native(address)
  
                            batches['clientAllowances'].append(
                                {
                                    "clientId": filNativeAddress,
                                    "allowance": functionParams.get('amount'),
                                    "verifierId": obj['msg']['To'],
                                    "msgCid": obj['baseMsg']['MsgCid'],
                                    "height": obj['height']
                                }
                            )

        
                # normalize addresses, replace everything with IDs (strip 'f0' prefix and convert to int)
                addressesToSearchInDb = []
                addressIdsToSearchInDb = []
                # go through each object in batches and collect all addresses

                fieldsToNormalize = ['clientId', 'verifierId', 'contractImmediateCaller'] 
                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalize:
                                if (field in item and item[field] is not None ): 
                                    if not address_is_id(item[field]):
                                        if not item[field] in addressesToSearchInDb:
                                            addressesToSearchInDb.append(item[field])  
                                    else:  
                                        addressId = address_to_id(item[field])
                                        if addressId != 0 and not addressId in addressIdsToSearchInDb:
                                            addressIdsToSearchInDb.append(addressId)

                print(f"Normalizing {len(addressesToSearchInDb)} addresses...")
                cur.execute(
                        "SELECT \"address\", \"addressId\" FROM public.actors WHERE \"address\" = ANY(%s);",
                        (addressesToSearchInDb,)
                    )
                rows = cur.fetchall()         
                print(f"Found {len(rows)} addresses in actors table for normalization.")

                address_mapping = {}
                for row in rows:
                    address_mapping[row[0]] = row[1]  # address -> id

                print(f"Normalizing {len(addressIdsToSearchInDb)} address IDs...")
                cur.execute(
                        "SELECT \"address\", \"addressId\" FROM public.actors WHERE \"addressId\" = ANY(%s);",
                        (addressIdsToSearchInDb,)
                    )
                rows = cur.fetchall()         
                print(f"Found {len(rows)} addresses in actors table for normalization.")

                addressId_mapping = {}
                for row in rows:
                    addressId_mapping[row[1]] = row[0]  # id -> address

                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalize:
                                if (field in item and item[field] is not None) :
                                    normalizedAddress, address_mapping, addressId_mapping = normalize_address_to_id(item[field], address_mapping, addressId_mapping)
                                    item[field] = normalizedAddress

                fieldsToNormalizeWithoutCaching = ['providerId']
                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalizeWithoutCaching:
                                if (field in item and item[field] is not None) :
                                    item[field] = address_to_id(item[field])

                # insert allocations
                if batches['allocations']:
                    cur.executemany(
                        "INSERT INTO public.allocations (\"allocationId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"expiration\", \"contractImmediateCaller\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (\"allocationId\") DO NOTHING;",
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
                                alloc['contractImmediateCaller']
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
                        allocationData = {'pieceCid': claim['pieceCid'], 'pieceSize': claim['pieceSize'], 'termMin': None, 'termMax': None}

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
                        "INSERT INTO public.deals (\"claimId\", \"clientId\", \"providerId\", \"sectorId\", \"dealId\", \"sectorExpiry\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"termStart\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;",
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
                                claim['termStart']
                            )
                            for claim in batches['claims']
                        ]
                    )

                # insert verifier allowances
                if batches['verifierAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verifier_allowance (\"verifierId\", allowance, \"msgCid\", \"height\") VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                va['verifierId'],
                                va['allowance'],
                                va['msgCid'],
                                va['height']
                            )
                            for va in batches['verifierAllowances']
                        ]
                    )

                # insert client allowances
                if batches['clientAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verified_client_allowance (\"verifierId\", \"clientId\", allowance, \"msgCid\", \"height\") VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                ca['verifierId'],
                                ca['clientId'],
                                ca['allowance'],
                                ca['msgCid'],
                                ca['height']
                            )
                            for ca in batches['clientAllowances']
                        ]
                    )

                # insert proposals
                if batches['proposals']:
                    cur.executemany(
                        "INSERT INTO public.deal_proposals (\"dealId\", \"clientId\", \"pieceCid\", \"pieceSize\", \"providerId\", label, verified, \"storagePricePerEpoch\", \"clientCollateral\", \"providerCollateral\", \"startEpoch\", \"endEpoch\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (\"dealId\") DO NOTHING;",
                        [
                            (
                                prop['dealId'],
                                prop['clientId'],
                                prop['pieceCid'],
                                prop['pieceSize'],
                                prop['providerId'],
                                prop['label'],
                                prop['verified'],
                                prop['storagePricePerEpoch'],
                                prop['clientCollateral'],
                                prop['providerCollateral'],
                                prop['startEpoch'],
                                prop['endEpoch']
                            )
                            for prop in batches['proposals']
                        ]
                    )

                # insert sector activations
                # Build array of deal ids from batches['sectorActivations']
                deal_ids = [sectorActivation['dealId'] for sectorActivation in batches['sectorActivations']]
                proposals_dict = {}
                if deal_ids:
                    # Select existing allocations from the table
                    cur.execute(
                       "SELECT \"dealId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"startEpoch\", \"endEpoch\", \"clientCollateral\", \"providerCollateral\", \"storagePricePerEpoch\", label, verified FROM public.deal_proposals WHERE \"dealId\" = ANY(%s);",
                        (deal_ids,)
                    )
                    rows = cur.fetchall()
                    # Build dictionary with id as key and object as value
                    for row in rows:
                        proposals_dict[row[0]] = {
                            'clientId': row[1],
                            'providerId': row[2],
                            'pieceCid': row[3],
                            'pieceSize': row[4],
                            'startEpoch': row[5],
                            'endEpoch': row[6],
                            'clientCollateral': row[7],
                            'providerCollateral': row[8],
                            'storagePricePerEpoch': row[9],
                            'label': row[10],
                            'verified': row[11],
                        }
                
                if batches['sectorActivations']:
                    dealsToInsert = []
                    sectorActivationsToInsert = []
                    
                    for sectorActivation in batches['sectorActivations']:
                        proposal = proposals_dict.get(sectorActivation['dealId'], None)
                        
                        if proposal:
                            dealsToInsert.append({
                                **sectorActivation,
                                "pieceCid": proposal['pieceCid'],
                                "pieceSize": proposal['pieceSize'],
                                "startEpoch": proposal['startEpoch'],
                                "endEpoch": proposal['endEpoch'],
                            })
                        else:
                            sectorActivationsToInsert.append(sectorActivation)
                            print(f"Warning: dealId {sectorActivation['dealId']} not found in deal proposals table.")

                    cur.executemany(
                        "INSERT INTO public.deals (\"claimId\", \"clientId\", \"providerId\", \"sectorId\", \"dealId\", \"sectorExpiry\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"termStart\") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;",
                        [
                            (
                                0,
                                1,
                                deal['providerId'],
                                deal['sectorNumber'],
                                deal['dealId'],
                                0,
                                deal['pieceCid'],
                                deal['pieceSize'],
                                deal['startEpoch'],
                                deal['endEpoch'],
                                deal['activationHeight']
                            )
                            for deal in dealsToInsert
                        ]
                    )

                    cur.executemany(
                        "INSERT INTO public.sector_activations (\"dealId\", \"providerId\", \"activationHeight\", \"sectorNumber\") VALUES (%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING;",
                        [
                            (
                                sectorActivation['dealId'],
                                sectorActivation['providerId'],
                                sectorActivation['activationHeight'],
                                sectorActivation['sectorNumber']
                            )
                            for sectorActivation in sectorActivationsToInsert
                        ]
                    )

                # insert metaAllocators
                if batches['metaAllocators']:
                    cur.executemany(
                        "INSERT INTO public.meta_allocators (\"addressId\", \"address\", \"addressEth\") VALUES (%s,%s,%s) ON CONFLICT (\"addressId\") DO NOTHING;",
                        [
                            (
                                prop['addressId'],
                                prop['address'],
                                prop['addressEth'],
                            )
                            for prop in batches['metaAllocators']
                        ]
                    )

                # insert clientContracts
                if batches['clientContracts']:
                    cur.executemany(
                        "INSERT INTO public.client_contracts (\"addressId\", \"address\", \"addressEth\") VALUES (%s,%s,%s) ON CONFLICT (\"addressId\") DO NOTHING;",
                        [
                            (
                                prop['addressId'],
                                prop['address'],
                                prop['addressEth'],
                            )
                            for prop in batches['clientContracts']
                        ]
                    )

       
    keys = list_keys()
    results = process_trace.expand(obj=keys) 
    decoded_results = decode_parameters.expand(messagesToDecode=results)
    output = output_results.expand(decodedResults=decoded_results)
    keys >> results >> decoded_results >> output