from datetime import datetime
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src")

from job3_analytics import run_daily_analytics

with DAG(
    "weather_daily_analytics",
    description="Daily analytics from SQLite weather data",
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["weather", "analytics", "sqlite"],
    max_active_runs=1,
) as dag:
    analytics_task = PythonOperator(
        task_id="compute_daily_weather_summary",
        python_callable=run_daily_analytics,
    )
