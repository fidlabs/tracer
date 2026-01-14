"""Combined DAG: local file enumeration with beryx-style processing and PostgreSQL storage."""
from __future__ import annotations
import os
import shlex
import subprocess
import json
import base64
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import psycopg
from web3 import Web3
from filecoin_address import encode, Address, CoinType, delegated_from_eth_address
from cbor2 import loads

from utils.trace_file_utils import find_cursor, plan_next_batch
from utils.trace_processing import (
    find_subcall,
    find_subcall_and_parent,
    find_subcall_and_path,
    get_network_version,
    find_actor_name,
    extract_dag_cid,
    eth_address_to_fil_native,
    address_is_id,
    address_to_id,
    normalize_address_to_id,
)
from utils.db_utils import (
    get_clickhouse_client,
    get_postgres_connection,
    claim_file_processing,
    mark_file_success,
    mark_file_failed,
)

# Configuration
TRACES_ROOT = Path(os.getenv("TRACES_ROOT", "/data/traces"))
START_SEQ = int(os.getenv("START_SEQ", "1"))
BATCH_MAX = int(os.getenv("BATCH_MAX", "1024"))
SHELL = "bash"


def process_trace_lines(traces_text: str, height: int) -> list[dict]:
    """
    Process trace lines with beryx-style subcall matching.
    
    Returns list of matched subcalls in the same format as beryx_parser's process_trace.
    """
    lines = traces_text.split('\n')
    matches = []
    print(f"Processing {len(lines)} trace objects at height {height}...")
    
    # Determine matchers based on height
    if height <= 3855360:
        matchers = [["f05", 4], ["f06", 2, 4], ["f04", 8]]
        matchersWithParents = []
        matchersWithPaths = []
        postNV22 = False
    else:
        matchers = [["f06", 2, 4, 3916220144], ["f07", 3621052141], ["f010", 3]]
        if height > 3996816:
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    rows = cur.execute("select \"address\", \"addressId\" from meta_allocators")
                    for row in rows:
                        matchers.append([row[0], 3844450837])
                        matchers.append([row[1], 3844450837])
                    rows = cur.execute("select \"address\", \"addressId\" from client_contracts")
                    for row in rows:
                        matchers.append([row[0], 3844450837])
                        matchers.append([row[1], 3844450837])
        
        matchersWithParents = [["f06", 9]]
        matchersWithPaths = [["f07", 80475954]]
        postNV22 = True
    
    for i, line in enumerate(lines):
        if line.strip():  # Skip empty lines
            try:
                trace_obj = json.loads(line)
                subcalls = []
                
                # Apply matchers
                for matcher in matchers:
                    if len(matcher) < 2:
                        continue
                    destination = matcher[0]
                    methods = matcher[1:]
                    for method in methods:
                        subcall = find_subcall(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method)
                        if subcall is not None:
                            subcalls.append({"subcall": subcall, "parent": None, "path": []})
                
                for matcher in matchersWithParents:
                    if len(matcher) < 2:
                        continue
                    destination = matcher[0]
                    methods = matcher[1:]
                    for method in methods:
                        subcall, parent = find_subcall_and_parent(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method, parent=None)
                        if subcall is not None:
                            subcalls.append({"subcall": subcall, "parent": parent, "path": []})
                
                for matcher in matchersWithPaths:
                    if len(matcher) < 2:
                        continue
                    destination = matcher[0]
                    methods = matcher[1:]
                    for method in methods:
                        subcall, path = find_subcall_and_path(subcalls=[trace_obj['ExecutionTrace']], destination=destination, method=method, path=[])
                        if subcall is not None:
                            subcalls.append({"subcall": subcall, "parent": None, "path": path})
                
                # Build matches from subcalls
                for match in subcalls:
                    if "subcall" in match:
                        msg = match["subcall"]['Msg']
                        msgRct = match["subcall"]['MsgRct']
                        verifRegUnivHookRct = []
                        if (msg['To'] == "f07" and msg['Method'] == 3621052141 and msg['From'] == "f05") or (msg['To'] == "f07" and msg['Method'] == 80475954):
                            verifregSubcall = find_subcall(subcalls=match["subcall"]['Subcalls'], destination="f06", method=3726118371)
                            if verifregSubcall:
                                if verifregSubcall['Msg']['From'] == 'f07':
                                    verifRegUnivHookRct = verifregSubcall['MsgRct']
                        
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
            
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON on line {i+1}: {e}")
                print(f"Line content: {line}")
    
    return matches


with DAG(
    dag_id="trace_seq_beryx_parser",
    start_date=datetime(2024, 1, 1),
    schedule="*/3 * * * *",  # Run every 3 minutes
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin", "s2", "postgres", "seq", "local", "beryx"]
) as dag:

    @task
    def init_postgres_schema() -> None:
        """Initialize PostgreSQL database and schema if not already exists."""
        print("=" * 60)
        print("TASK: init_postgres_schema")
        print("=" * 60)
        
        # Connect to default 'postgres' database to create 'filecoin' database
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname="postgres",  # Connect to default database
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            connect_timeout=10
        ) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create database if not exists
                print("Creating 'filecoin' database if it doesn't exist...")
                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'filecoin'")
                if not cur.fetchone():
                    cur.execute('CREATE DATABASE filecoin')
                    print("✓ Created 'filecoin' database")
                else:
                    print("✓ 'filecoin' database already exists")
        
        # Connect to 'filecoin' database and run schema
        print("Initializing schema in 'filecoin' database...")
        schema_path = "/opt/airflow/include/sql/ps_filecoin_schema.sql"
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        
        # Split by semicolon and execute each statement
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                # Split SQL by semicolon, but be careful with semicolons in strings
                statements = []
                current_stmt = ""
                for line in schema_sql.split('\n'):
                    line = line.strip()
                    if not line or line.startswith('--'):
                        continue
                    current_stmt += line + '\n'
                    if line.endswith(';'):
                        stmt = current_stmt.strip()
                        if stmt:
                            statements.append(stmt)
                        current_stmt = ""
                
                # Execute each statement
                for i, stmt in enumerate(statements, 1):
                    try:
                        cur.execute(stmt)
                        print(f"✓ Executed statement {i}/{len(statements)}")
                    except Exception as e:
                        # Ignore "already exists" errors
                        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                            print(f"⚠ Statement {i} skipped (already exists): {str(e)[:100]}")
                        else:
                            print(f"❌ Error in statement {i}: {e}")
                            raise
                
                conn.commit()
                print("✅ Schema initialization complete")

    @task
    def find_cursor_task() -> int:
        """Find last successful seq_num from ClickHouse."""
        print("=" * 60)
        print("TASK: find_cursor")
        print("=" * 60)
        ch_client = get_clickhouse_client()
        cursor = find_cursor(ch_client, START_SEQ)
        print(f"✓ Found cursor: {cursor}")
        print(f"✓ Next batch will start from seq {cursor + 1}")
        return cursor

    @task
    def plan_next_batch_task(prev: int) -> list[dict]:
        """Plan next batch of files to process."""
        print("=" * 60)
        print("TASK: plan_next_batch")
        print("=" * 60)
        print(f"Previous cursor: {prev}")
        print(f"Traces root: {TRACES_ROOT}")
        batch = plan_next_batch(prev, TRACES_ROOT, BATCH_MAX)
        print(f"✓ Found {len(batch)} files to process")
        if len(batch) > 0:
            print(f"✓ Range: seq {batch[0]['seq']} to {batch[-1]['seq']}")
        return batch

    @task
    def fetch_and_process_traces(obj: dict) -> list[dict]:
        """
        Claim file, decompress, extract traces, and process with beryx-style matching.
        
        Returns list of matched subcalls (same format as beryx_parser's process_trace).
        """
        seq = obj["seq"]
        path = obj["path"]
        etag = obj["etag"]
        height = seq  # seq_num is the same as height
        
        print(f"[seq {seq}] Starting fetch and process...")
        print(f"[seq {seq}] File: {path}")
        print(f"[seq {seq}] Height: {height}")
        print(f"[seq {seq}] ETag: {etag}")
        
        ch_client = get_clickhouse_client()
        
        # Claim file for processing
        should_process, attempts = claim_file_processing(ch_client, seq, etag)
        if not should_process:
            print(f"[seq {seq}] ⏭ SKIPPED: Already successfully processed with this ETag")
            return []  # Return empty list to skip downstream tasks
        
        print(f"[seq {seq}] ✓ Task claimed (attempt #{attempts})")
        
        try:
            # Decompress and extract traces
            print(f"[seq {seq}] Decompressing and extracting traces...")
            # Use jq's // operator to default to empty array if Trace is null
            # This prevents "Cannot iterate over null" errors
            src = f"/usr/local/bin/s2d -c {shlex.quote(path)} | /usr/bin/jq -c '.Trace // [] | .[]'"
            proc = subprocess.run(
                src,
                shell=True,
                executable=SHELL,
                capture_output=True,
                text=True
            )
            
            if proc.returncode != 0:
                raise RuntimeError(f"Pipeline failed for seq {seq}: {proc.stderr}")
            
            traces_text = proc.stdout.strip()
            if not traces_text:
                print(f"[seq {seq}] ⚠ No traces found in file (Trace array was null or empty)")
                mark_file_success(ch_client, seq, etag, attempts)
                return []
            
            # Process traces
            print(f"[seq {seq}] Processing traces...")
            matches = process_trace_lines(traces_text, height)
            print(f"[seq {seq}] ✓ Found {len(matches)} matched subcalls")
            
            # Mark success
            mark_file_success(ch_client, seq, etag, attempts)
            print(f"[seq {seq}] ✅ SUCCESS: Marked as completed")
            
            return matches
        
        except Exception as e:
            print(f"[seq {seq}] ❌ ERROR: {e}")
            mark_file_failed(ch_client, seq, etag, attempts, str(e))
            raise

    @task
    def decode_parameters(messagesToDecode: list[dict]) -> list[dict]:
        """
        Decode parameters using the Go executable.
        
        Same as beryx_parser's decode_parameters task.
        """
        decodedResults = []
        for obj in messagesToDecode:
            try:
                print("--------------------------------")
                print("DECODING PARAMETERS")
                print(obj)
                print("--------------------------------")
                actorName = find_actor_name(obj['msg']['To'])
                if 'ParamsCodec' not in obj['msg'] or obj['msg']['ParamsCodec'] == 81:
                    decodeParametersCmd = [
                        "/opt/airflow/plugins/goExecutables/decode_params",
                        actorName,
                        str(obj['msg']['Method']),
                        obj['msg']['Params'],
                        get_network_version(obj['height'])
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
                        get_network_version(obj['height'])
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
                "msgRct": obj['msgRct'],
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
    def output_results(decodedResults: list[dict]) -> None:
        """
        Insert results into PostgreSQL.
        
        Same as beryx_parser's output_results task.
        """
        with get_postgres_connection() as conn:
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

                batches = {
                    "allocations": [],
                    "claims": [],
                    "verifierAllowances": [],
                    "clientAllowances": [],
                    "proposals": [],
                    "sectorActivations": [],
                    "metaAllocators": [],
                    "clientContracts": []
                }
                
                for obj in decodedResults:
                    # Skip if decodedParams is None (no parameters to decode)
                    if obj['decodedParams'] is None:
                        print(f"Skipping message with no decodedParams: {obj['msg'].get('To', 'unknown')} method {obj['msg'].get('Method', 'unknown')}")
                        continue
                    
                    # allocation
                    if (obj['msg']['To'] == "f07" and obj['msg']['Method'] == 3621052141 and obj['msg']['From'] == "f05") or (obj['msg']['To'] == "f07" and obj['msg']['Method'] == 80475954):
                        decodedReceipt = loads(base64.b64decode(obj['verifRegUnivHookRct']['Return']))
                        decodedParams = loads(base64.b64decode(obj['decodedParams']['OperatorData']))
                        clientId = ''
                        if obj['msg']['Method'] == 3621052141:
                            clientId = obj['decodedParams']['From']
                        if obj['msg']['Method'] == 80475954:
                            clientId = obj['msg']['From']

                        contractImmediateCaller = None
                        if (len(obj['path']) > 1):
                            contractImmediateCaller = obj['path'][-2]['from']
                        else:
                            contractImmediateCaller = obj['msg']['From']

                        allocations = decodedParams[0]
                        for i in range(len(allocations)):
                            allocation = allocations[i]
                            allocationId = decodedReceipt[2][i]
                            batches['allocations'].append({
                                'id': allocationId,
                                'clientId': int(clientId[2:]),
                                'providerId': allocation[0],
                                'pieceCid': extract_dag_cid(allocation[1]),
                                'pieceSize': allocation[2],
                                'termMin': allocation[3],
                                'termMax': allocation[4],
                                'expiration': allocation[5],
                                'contractImmediateCaller': contractImmediateCaller,
                            })

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
                                    sectorNumber = sector['SectorNumber']
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
                                    batches['claims'].append({
                                        'id': claim['AllocationId'],
                                        'clientId': claim['Client'],
                                        'pieceCid': claim['Data']['/'],
                                        'pieceSize': claim['Size'],
                                        'providerId': int(obj['msg']['From'][2:]),
                                        'sector': sector['Sector'],
                                        'dealId': dealId,
                                        'sectorExpiry': sector['SectorExpiry'],
                                        'termStart': obj['height'],
                                    })

                    # create verifier (f080 multisig)
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 2:
                        batches['verifierAllowances'].append({
                            "verifierId": obj['decodedParams']['Address'],
                            "allowance": obj['decodedParams']['Allowance'],
                            "msgCid": obj['baseMsg']['MsgCid'],
                            "height": obj['height'],
                            "type": "direct"
                        })

                    # create client meta-allocator
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 3916220144:
                        batches['clientAllowances'].append({
                            "clientId": obj['decodedParams']['Address'],
                            "allowance": obj['decodedParams']['Allowance'],
                            "verifierId": obj['msg']['From'],
                            "height": obj['height'],
                            "msgCid": obj['baseMsg']['MsgCid'],
                            "type": "meta-allocator"
                        })

                    # create client
                    if obj['msg']['To'] == "f06" and obj['msg']['Method'] == 4:
                        batches['clientAllowances'].append({
                            "clientId": obj['decodedParams']['Address'],
                            "allowance": obj['decodedParams']['Allowance'],
                            "verifierId": obj['msg']['From'],
                            "height": obj['height'],
                            "msgCid": obj['baseMsg']['MsgCid'],
                            "type": "direct"
                        })

                    # proposal
                    if obj['msg']['To'] == "f05" and obj['msg']['Method'] == 4:
                        decodedReceipt = loads(base64.b64decode(obj['msgRct']['Return']))
                        proposalIndex = 0
                        for deal in obj['decodedParams']['Deals']:
                            proposal = deal['Proposal']
                            if proposal['VerifiedDeal'] is True:
                                batches['proposals'].append({
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
                                })
                            proposalIndex += 1

                    # sector activations
                    if obj['msg']['To'] == "f04" and obj['msg']['Method'] == 8:
                        if obj['decodedParams'] and obj['decodedParams'].get('DealIDs') is not None:
                            for dealId in obj['decodedParams']['DealIDs']:
                                batches['sectorActivations'].append({
                                    'dealId': dealId,
                                    'providerId': int(obj['decodedParams']['Miner']),
                                    'activationHeight': obj['height'],
                                    'sectorNumber': int(obj['decodedParams']['Number'])
                                })

                    # sector activations
                    if obj['msg']['To'] == "f010" and obj['msg']['Method'] == 3 and obj['msgRct']['ExitCode'] == 0 and (obj['msg']['From'] == "f03239905" or obj['msg']['From'] == "f03136590"):
                        decodedReceipt = loads(base64.b64decode(obj['msgRct']['Return']))

                        addressId = decodedReceipt[0]
                        address = Address(decodedReceipt[1], CoinType.MAIN)
                        addressEth = decodedReceipt[2]

                        if (obj['msg']['From'] == "f03239905"):
                            batches['clientContracts'].append({
                                'addressId': addressId,
                                'address': delegated_from_eth_address("0x" + addressEth.hex()),
                                'robustAddress': encode('f', address),
                                'addressEth': "0x" + addressEth.hex()
                            })

                        if (obj['msg']['From'] == "f03136590"):
                            batches['metaAllocators'].append({
                                'addressId': addressId,
                                'address': delegated_from_eth_address("0x" + addressEth.hex()),
                                'robustAddress': encode('f', address),
                                'addressEth': "0x" + addressEth.hex()
                            })

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

                            batches['verifierAllowances'].append({
                                "verifierId": filNativeAddress,
                                "allowance": functionParams.get('amount'),
                                "msgCid": obj['baseMsg']['MsgCid'],
                                "height": obj['height'],
                                "type": "meta-allocator"
                            })

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

                            batches['clientAllowances'].append({
                                "clientId": filNativeAddress,
                                "allowance": functionParams.get('amount'),
                                "verifierId": obj['msg']['To'],
                                "msgCid": obj['baseMsg']['MsgCid'],
                                "height": obj['height'],
                                "type": "contract"
                            })

                # Normalize addresses
                addressesToSearchInDb = []
                addressIdsToSearchInDb = []

                fieldsToNormalize = ['clientId', 'verifierId', 'contractImmediateCaller']
                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalize:
                                if (field in item and item[field] is not None):
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
                    address_mapping[row[0]] = row[1]

                print(f"Normalizing {len(addressIdsToSearchInDb)} address IDs...")
                cur.execute(
                    "SELECT \"address\", \"addressId\" FROM public.actors WHERE \"addressId\" = ANY(%s);",
                    (addressIdsToSearchInDb,)
                )
                rows = cur.fetchall()
                print(f"Found {len(rows)} addresses in actors table for normalization.")

                addressId_mapping = {}
                for row in rows:
                    addressId_mapping[row[1]] = row[0]

                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalize:
                                if (field in item and item[field] is not None):
                                    normalizedAddress, address_mapping, addressId_mapping = normalize_address_to_id(item[field], address_mapping, addressId_mapping)
                                    item[field] = normalizedAddress

                fieldsToNormalizeWithoutCaching = ['providerId']
                for key in batches:
                    if batches[key]:
                        for item in batches[key]:
                            for field in fieldsToNormalizeWithoutCaching:
                                if (field in item and item[field] is not None):
                                    item[field] = address_to_id(item[field])

                # Insert allocations
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

                # Insert claims
                claim_ids = [claim['id'] for claim in batches['claims']]
                allocations_dict = {}
                if claim_ids:
                    cur.execute(
                        "SELECT \"allocationId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"termMin\", \"termMax\", \"expiration\" FROM public.allocations WHERE \"allocationId\" = ANY(%s);",
                        (claim_ids,)
                    )
                    rows = cur.fetchall()
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

                # Insert verifier allowances
                if batches['verifierAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verifier_allowance (\"verifierId\", allowance, \"msgCid\", \"height\", \"type\") VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                va['verifierId'],
                                va['allowance'],
                                va['msgCid'],
                                va['height'],
                                va['type']
                            )
                            for va in batches['verifierAllowances']
                        ]
                    )

                # Insert client allowances
                if batches['clientAllowances']:
                    cur.executemany(
                        "INSERT INTO public.verified_client_allowance (\"verifierId\", \"clientId\", allowance, \"msgCid\", \"height\", \"type\") VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;",
                        [
                            (
                                ca['verifierId'],
                                ca['clientId'],
                                ca['allowance'],
                                ca['msgCid'],
                                ca['height'],
                                ca['type']
                            )
                            for ca in batches['clientAllowances']
                        ]
                    )

                # Insert proposals
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

                # Insert sector activations
                deal_ids = [sectorActivation['dealId'] for sectorActivation in batches['sectorActivations']]
                proposals_dict = {}
                if deal_ids:
                    cur.execute(
                        "SELECT \"dealId\", \"clientId\", \"providerId\", \"pieceCid\", \"pieceSize\", \"startEpoch\", \"endEpoch\", \"clientCollateral\", \"providerCollateral\", \"storagePricePerEpoch\", label, verified FROM public.deal_proposals WHERE \"dealId\" = ANY(%s);",
                        (deal_ids,)
                    )
                    rows = cur.fetchall()
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

                # Insert metaAllocators
                if batches['metaAllocators']:
                    cur.executemany(
                        "INSERT INTO public.meta_allocators (\"addressId\", \"address\", \"addressEth\", \"robustAddress\") VALUES (%s,%s,%s,%s) ON CONFLICT (\"addressId\") DO NOTHING;",
                        [
                            (
                                prop['addressId'],
                                prop['address'],
                                prop['addressEth'],
                                prop['robustAddress']
                            )
                            for prop in batches['metaAllocators']
                        ]
                    )

                # Insert clientContracts
                if batches['clientContracts']:
                    cur.executemany(
                        "INSERT INTO public.client_contracts (\"addressId\", \"address\", \"addressEth\", \"robustAddress\") VALUES (%s,%s,%s,%s) ON CONFLICT (\"addressId\") DO NOTHING;",
                        [
                            (
                                prop['addressId'],
                                prop['address'],
                                prop['addressEth'],
                                prop['robustAddress']
                            )
                            for prop in batches['clientContracts']
                        ]
                    )

    # Task flow
    init = init_postgres_schema()
    prev = find_cursor_task()
    batch = plan_next_batch_task(prev)
    results = fetch_and_process_traces.expand(obj=batch)
    decoded_results = decode_parameters.expand(messagesToDecode=results)
    output = output_results.expand(decodedResults=decoded_results)
    
    init >> prev >> batch >> results >> decoded_results >> output

