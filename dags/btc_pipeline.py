from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import csv
import logging
from datetime import datetime

import ccxt

default_args = {
    "owner": "mumaroi",
    "start_date": timezone.datetime(2022, 7, 1),
}
with DAG(
    "btc_pipeline",
    default_args=default_args,
    schedule_interval=None,
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

    sampleTask = PythonOperator(
        task_id="fetch_btc",
        python_callable=fetch_btc
    )
