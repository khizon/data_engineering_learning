from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz
from utils import *

# Set the timezone to Tokyo (UTC+9)
tokyo_tz = pytz.timezone('Asia/Tokyo')


default_args = {
    'owner': 'kiel',
    'start_date': datetime(2023, 10, 15, 2, 0, 0, tzinfo=tokyo_tz),
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id="ingest_MAL_data_v0",
    schedule_interval='@daily'
) as dag:

    task_ingest_top_airing_anime = PythonOperator(
        task_id = 'ingest_top_airing_anime',
        python_callable = ingest_top_airing_anime,
        trigger_rule='all_failed',
    )

    task_update_top_airing_anime = PythonOperator(
        task_id = 'update_top_airing_anime',
        python_callable = update_top_airing_anime,
        # trigger_rule='all_failed',
    )

    task_ingest_top_airing_anime >> task_update_top_airing_anime