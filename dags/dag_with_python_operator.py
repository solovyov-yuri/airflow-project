from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "Yuri Solovyov",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


def get_name():
    return "Max"


def get_fullname(ti):
    ti.xcom_push(key="first_name", value="Max")
    ti.xcom_push(key="last_name", value="Tegmark")


def get_age(ti):
    ti.xcom_push(key="age", value=56)


def greet(ti):
    first_name = ti.xcom_pull(task_ids="get_fullname", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_fullname", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello. My name is {first_name} {last_name}, I'am {age} years old")


with DAG(
    dag_id="dag_with_python_operator_v06",
    default_args=default_args,
    description="DAG with python operator",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 3, 7),
    tags=["python operator example"],
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        # op_kwargs={"name": "John", age=53},
    )

    task2 = PythonOperator(
        task_id="get_fullname", python_callable=get_fullname
    )

    task3 = PythonOperator(task_id="get_age", python_callable=get_age)

    [task2, task3] >> task1
