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
    dag_id="ingest_MAL_info_v0",
    schedule_interval='@weekly'
) as dag:

    task_ingest_anime_info = PythonOperator(
        task_id = 'ingest_anime_info',
        python_callable = ingest_anime_info,
        trigger_rule='all_failed',
    )

    task_update_anime_info = PythonOperator(
        task_id = 'update_anime_info',
        python_callable = update_anime_info,
        trigger_rule='all_failed',
    )

    task_ingest_anime_info >> task_update_anime_info