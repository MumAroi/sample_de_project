from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone

default_args = {
    "owner": "mumaroi",
    "start_date": timezone.datetime(2022, 7, 1),
}
with DAG(
    "btc_pipeline",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    sampleTask = DummyOperator(
        task_id="sample_task",
    )
