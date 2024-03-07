from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Yuri Solovyov",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="first_dag",
    default_args=default_args,
    description="This is my first dag",
    start_date=datetime(2024, 3, 7),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="first_task",
        bash_command="echo hello world, this is the first task",
    )

    task1
