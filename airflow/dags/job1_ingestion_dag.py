from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

sys.path.insert(0, "/opt/airflow/src")

from job1_producer import run_producer

with DAG(
    dag_id="weather_continuous_ingestion",
    description="Continuous ingestion: Weather API → Kafka (pseudo-streaming)",
    schedule_interval="@once",  # один запуск → дальше работает постоянно
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["weather", "kafka", "streaming"],
    max_active_runs=1,
) as dag:
    ingest_task = PythonOperator(
        task_id="run_weather_producer",
        python_callable=run_producer,
    )
