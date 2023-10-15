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
    'start_date': datetime(2023, 10, 14, tzinfo=tokyo_tz),
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id="ingest_MAL_data",
    schedule_interval='@daily'
) as dag:

    task_ingest_top_airing_anime = PythonOperator(
        task_id = 'ingest_top_airing_anime',
        python_callable = ingest_top_airing_anime
    )

    task_update_top_airing_anime = PythonOperator(
        task_id = 'update_top_airing_anime',
        python_callable = update_top_airing_anime
    )

    task_ingest_anime_info = PythonOperator(
        task_id = 'ingest_anime_info',
        python_callable = ingest_anime_info
    )

    task_update_anime_info = PythonOperator(
        task_id = 'update_anime_info',
        python_callable = update_anime_info
    )

    task_ingest_top_airing_anime >> task_update_top_airing_anime
    task_update_top_airing_anime >> task_ingest_anime_info
    task_ingest_anime_info >> task_update_anime_info