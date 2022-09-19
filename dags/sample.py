import json
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 5, 23),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "sample",
    default_args=default_args,
    schedule_interval="* * * * *",
    max_active_runs=1,
)


p1 = DummyOperator(task_id="p1", dag=dag)
p2 = DummyOperator(task_id="p2", dag=dag)

p1 >> p2