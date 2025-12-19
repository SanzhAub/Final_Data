from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# Добавляем путь к src
sys.path.insert(0, "/opt/airflow/src")

# Импортируем функцию продюсера
from job1_producer import run_producer

# Определяем DAG
with DAG(
    "almaty_weather_ingestion",
    description="Сбор данных о погоде в Алматы и отправка в Kafka",
    schedule_interval=None,  # Запускается вручную
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["weather", "kafka", "ingestion"],
) as dag:
    # Задача: запуск продюсера
    producer_task = PythonOperator(
        task_id="collect_and_send_weather_data",
        python_callable=run_producer,
    )
