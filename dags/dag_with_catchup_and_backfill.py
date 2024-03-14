from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "Yuri Solovyov",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_with_catchup_backfill_v02",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=["example"],
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo Simple bash command"
    )
