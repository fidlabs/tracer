from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from web3 import Web3
from web3.contract import Contract
from filecoin_address import  encode, Address, CoinType

default_args = {
   'owner' : 'your name',
   'retries' : 5,
   'retry_delay' : timedelta(minutes=2),
}

with DAG(
    dag_id="go_datacapstats",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["filecoin","r2","s2","clickhouse","exactly-once-ish"]
) as dag:

     
    @task
    def output_results() -> None:
        import json
        
        # Load the ABI from file
        with open('/opt/airflow/plugins/abis/meta-allocator.json', 'r') as f:
            abi = json.load(f)
        
        w3 = Web3()
        contract = w3.eth.contract(address=Web3.to_checksum_address('0x984376abd1ff5518b6ae9d065c40696ae916dc88'), abi=abi)
        res = contract.decode_function_input('0x930fc0060000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000150176e3f30cc8fb2febd011b5737fa8ed5dfbb097050000000000000000000000')
        print(res[0].fn_name)

        address = Address(res[1].get('clientAddress'), CoinType.MAIN)
        print(encode('f', address))
        print(res[1].get('amount'))


    output = output_results()
    output