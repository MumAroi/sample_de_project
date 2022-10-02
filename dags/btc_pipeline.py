from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator

from datetime import timedelta

import csv
import logging
from datetime import datetime

import ccxt

default_args = {
    "owner": "mumaroi",
    "email": ["mmaroi@mailhog.io"],
    "start_date": timezone.datetime(2022, 7, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "btc_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
) as dag:

    def fetch_btc(**context):
        ds = context["ds"]

        #Binance exchange
        exchange = ccxt.binance()

        #Fetch data
        dt_obj = datetime.strptime(ds, "%Y-%m-%d")
        millisec = int(dt_obj.timestamp() * 1000)
        ohlcv = exchange.fetch_ohlcv("BTC/USDT", timeframe="1h", since=millisec, limit=24)
        logging.info(f"OHLCV of BTC/USDT value. [ohlcv={ohlcv}]")

        #Create data to CSV file
        with open(f"btc-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerows(ohlcv)

        #Store CSV file in datalake
        s3_hook = S3Hook(aws_conn_id="myminio")
        s3_hook.load_file(
            f"btc-{ds}.csv",
            key=f"cryptocurrency/{ds}/btc.csv",
            bucket_name="data-lake",
            replace=True,
        )

    def download_btc_file(**context):
        ds = context["ds"]

        # Download file from minios3
        s3_hook = S3Hook(aws_conn_id="myminio")
        file_name = s3_hook.download_file(
            key=f"cryptocurrency/{ds}/btc.csv",
            bucket_name="data-lake",
        )

        return file_name

    def load_btc_into_database(**context):
        postgres_hook = PostgresHook(postgres_conn_id="mypostgres")
        conn = postgres_hook.get_conn()

        # Get file name from XComs
        file_name = context["ti"].xcom_pull(task_ids="dowload_btc_data_file", key="return_value")

        # Copy file to database
        postgres_hook.copy_expert(
            """
                COPY
                    btc_import
                FROM STDIN DELIMITER ',' CSV
            """,
            file_name,
        )

    fetch_btc_data = PythonOperator(
        task_id="fetch_btc_data",
        python_callable=fetch_btc
    )

    dowload_btc_data_file = PythonOperator(
        task_id="dowload_btc_data_file",
        python_callable=download_btc_file
    )

    create_btc_import_table = PostgresOperator(
        task_id="create_btc_import_table",
        postgres_conn_id="mypostgres",
        sql="""
            CREATE TABLE IF NOT EXISTS btc_import (
                timestamp BIGINT,
                open FLOAT,
                highest FLOAT,
                lowest FLOAT,
                closing FLOAT,
                volume FLOAT
            )
        """,
    )

    load_btc_data_into_database = PythonOperator(
        task_id="load_btc_data_into_database",
        python_callable=load_btc_into_database,
    )

    create_btc_final_table = PostgresOperator(
        task_id="create_btc_final_table",
        postgres_conn_id="mypostgres",
        sql="""
            CREATE TABLE IF NOT EXISTS btc (
                timestamp BIGINT PRIMARY KEY,
                open FLOAT,
                highest FLOAT,
                lowest FLOAT,
                closing FLOAT,
                volume FLOAT
            )
        """,
    )

    merge_btc_into_final_table = PostgresOperator(
        task_id="merge_btc_into_final_table",
        postgres_conn_id="mypostgres",
        sql="""
            INSERT INTO btc (
                timestamp,
                open,
                highest,
                lowest,
                closing,
                volume
            )
            SELECT
                timestamp,
                open,
                highest,
                lowest,
                closing,
                volume
            FROM
                btc_import
            ON CONFLICT (timestamp)
            DO UPDATE SET
                open = EXCLUDED.open,
                highest = EXCLUDED.highest,
                lowest = EXCLUDED.lowest,
                closing = EXCLUDED.closing,
                volume = EXCLUDED.volume
        """,
    )

    clear_btc_import_table = PostgresOperator(
        task_id="clear_btc_import_table",
        postgres_conn_id="mypostgres",
        sql="""
            DELETE FROM btc_import
        """,
    )

    notify = EmailOperator(
        task_id="notify",
        to=["mmaroi@mailhog.io"],
        subject="Loaded data into database successfully on {{ ds }}",
        html_content="Your pipeline has loaded data into database successfully",
    )

fetch_btc_data >> dowload_btc_data_file >> create_btc_import_table >> load_btc_data_into_database 
load_btc_data_into_database >> create_btc_final_table >> merge_btc_into_final_table 
merge_btc_into_final_table >> clear_btc_import_table >> notify