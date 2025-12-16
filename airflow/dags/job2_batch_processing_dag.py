import os
import sys
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

sys.path.insert(0, '/opt/airflow/src')


from job2_processor import run_batch_processor

with DAG(
    'weather_batch_processing',
    description='Batch job: чтение из Kafka, очистка данных, запись в SQLite',
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['weather', 'kafka', 'batch', 'sqlite', 'processing'],
    max_active_runs=1,
) as dag:
    
    batch_process_task = PythonOperator(
        task_id='process_weather_batch',
        python_callable=run_batch_processor,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

