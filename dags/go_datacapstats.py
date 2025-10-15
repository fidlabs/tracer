from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


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

   run_go_task = BashOperator(
       task_id='run_go_task',
       bash_command='/opt/airflow/plugins/goExecutables/datacapstats',
   )