"""Trace processing utilities for subcall matching, address normalization, and decoding."""
import os
import requests
import psycopg
from filecoin_address import delegated_from_eth_address
from cbor2 import CBORTag, loads
from multiformats import CID


LOTUS_API_URL = os.environ.get("LOTUS_API_URL")
LOTUS_AUTH_TOKEN = os.environ.get("LOTUS_AUTH_TOKEN")


def eth_address_to_fil_native(address: str) -> str:
    """Convert Ethereum address to Filecoin native address."""
    has_prefix = address.lower().startswith('0xff0000000000000000000000')
    if has_prefix:
        filNativeAddress = "f0" + str(int.from_bytes(bytes.fromhex(address[26:]), byteorder="big", signed=False))
    else:
        filNativeAddress = delegated_from_eth_address(address)
    return filNativeAddress


def address_is_id(address: str | int) -> bool:
    """Check if address is ID format (f0/t0 or int)."""
    if isinstance(address, int):
        return True
    if isinstance(address, str) and (address.startswith("f0") or address.startswith("t0")):
        return True
    return False


def address_to_id(address: str | int) -> int:
    """Convert address to ID (strip f0/t0 prefix or return int)."""
    if isinstance(address, int):
        return address
    if isinstance(address, str) and (address.startswith("f0") or address.startswith("t0")):
        return int(address[2:])
    return 0


def lotus_api_call(method: str, params: list):
    """Make a Lotus API call."""
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


def normalize_address_to_id(address: str | int, addressDictionary, addressIdDictionary) -> tuple[int, dict, dict]:
    """
    Normalize address to ID with Lotus API calls if needed.
    
    Returns: (address_id, updated_addressDictionary, updated_addressIdDictionary)
    """
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


def get_network_version(height: int) -> str:
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
    """Map actor ID to name."""
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


def find_subcall(subcalls, destination, method):
    """Find subcall in trace tree by destination and method."""
    if subcalls is None or destination is None or method is None:
        return None
    for subcall in subcalls:
        # Skip subcalls without a valid Msg structure
        if not subcall or 'Msg' not in subcall or subcall.get('Msg') is None:
            continue
        
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


def find_subcall_and_parent(subcalls, destination, method, parent=None):
    """Find subcall with parent context."""
    if subcalls is None or destination is None or method is None:
        return None, None
    for subcall in subcalls:
        # Skip subcalls without a valid Msg structure
        if not subcall or 'Msg' not in subcall or subcall.get('Msg') is None:
            continue
        
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if destination matches and method is in the list of methods
        if msg_to == destination and msg_method == method:
            return subcall, parent
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall_and_parent(subcall['Subcalls'], destination, method, subcall)
            if result[0] is not None:
                return result
   
    return None, None


def find_subcall_and_path(subcalls, destination, method, path=[]):
    """Find subcall with call path."""
    if subcalls is None or destination is None or method is None:
        return None, None
    for subcall in subcalls:
        # Skip subcalls without a valid Msg structure
        if not subcall or 'Msg' not in subcall or subcall.get('Msg') is None:
            continue
        
        msg_to = subcall['Msg']['To']
        msg_method = subcall['Msg']['Method']
        
        # Check if destination matches and method is in the list of methods
        if msg_to == destination and msg_method == method:
            return subcall, path
        
        # Recursively check nested subcalls if they exist
        if subcall.get('Subcalls'):
            result = find_subcall_and_path(subcall['Subcalls'], destination, method, path + [{"to": subcall['Msg']['To'], "from": subcall['Msg']['From']}])
            if result[0] is not None:
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

