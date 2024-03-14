from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "Yuri Solovyov",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_with_cron_expression_v04",
    default_args=default_args,
    schedule_interval="0 3 * * Tue,Fri",
    start_date=datetime(2024, 3, 1),
    tags=["example"],
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo dag with cron expression"
    )
