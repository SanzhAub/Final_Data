from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, "/opt/airflow/src")
from job1_producer import run_producer

with DAG(
    dag_id="weather_continuous_ingestion",
    description="Pseudo-streaming: Weather API → Kafka",
    schedule_interval="*/1 * * * *",  # запуск каждую минуту
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["weather", "kafka", "streaming"],
    max_active_runs=1,
) as dag:
    ingest_task = PythonOperator(
        task_id="run_weather_producer",
        python_callable=run_producer,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )
