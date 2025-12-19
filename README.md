# Weather Data Pipeline

## Project Overview

This project implements a **data pipeline** for collecting, processing, and analyzing weather data using **Airflow**, **Kafka**, **Pandas**, and **SQLite**.
The pipeline fetches real-time weather data from **WeatherAPI**, cleans and stores it in a database, and generates daily analytics.

---

## Features

* DAG 1: Data Ingestion** – fetches current weather data and sends raw events to Kafka (`raw_weather_events`).
* DAG 2: Batch Processing** – reads data from Kafka, cleans and validates it, stores in SQLite (`events` table).
* DAG 3: Daily Analytics** – aggregates data from SQLite, calculates daily min/max/avg temperature and average humidity, stores in `daily_weather_summary`.

---

## Requirements

* Python >= 3.10
* Docker & Docker Compose
* Kafka & Zookeeper (via docker-compose)
* Pandas
* SQLite3
* requests

---

## Setup with Docker

1. Clone the repository:

```bash
git clone <repository_url>
cd project
```

2. Build and start the services using **docker-compose**:

```bash
docker-compose up --build
```

3. Open **Airflow UI**: [http://localhost:8080](http://localhost:8080)

   * Default login: `airflow` / `airflow`

4. Trigger DAGs:

   * DAG 1: `dag1_weather_ingestion`
   * DAG 2: `dag2_weather_batch`
   * DAG 3: `dag3_daily_analytics`

---

## Project Structure

```
project/
│ README.md
│ requirements.txt
├─ src/
│   ├─ job1_producer.py
│   ├─ job2_processor.py
│   ├─ job3_analytics.py
│   └─ database.py
├─ airflow/
│   └─ dags/
│       ├─ dag1_weather_ingestion.py
│       ├─ dag2_weather_batch.py
│       └─ dag3_daily_analytics.py
├─ data/
│   └─ weather.db
├─ docker-compose.yml
└─ report/
    └─ report.pdf
```

---

## Usage

* Start Docker services to launch *Airflow*, *Kafka*, and *Zookeeper*.
* Run *DAG 1* to ingest data → Kafka.
* Run *DAG 2* to process data → SQLite.
* Run *DAG 3* to generate daily summaries → SQLite.

---

## Notes

* For testing, DAG 1 can be configured to limit the number of iterations.
* SQLite is used for simplicity; you can switch to other DBs if needed.
* Make sure to configure your *WeatherAPI key* in `job1_producer.py` or as an environment variable.

---

