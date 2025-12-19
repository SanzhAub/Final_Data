import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import sqlite3

# Импортируем модуль базы данных
from database import get_db_path, init_database


def read_from_kafka(
    kafka_servers: str,
    topic: str,
    consumer_group: str = "weather_batch_processor",
    hours_back: int = 1,
) -> List[Dict[str, Any]]:
    messages = []

    try:
        start_time = datetime.utcnow() - timedelta(hours=hours_back)

        print(f"Подключение к Kafka: {kafka_servers}")
        print(f"Топик: {topic}")
        print(f"Consumer Group: {consumer_group}")
        print(f"Читаем сообщения с {start_time.isoformat()}")

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            group_id=consumer_group,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=30000,
        )

        print("Подключено к Kafka. Читаем сообщения...")
        partitions = consumer.assignment()
        if partitions:
            for partition in partitions:
                position = consumer.position(partition)
                print(f"Партиция {partition}: текущая позиция offset = {position}")
                if position == 0:
                    print(f"Начинаем чтение с начала партиции {partition}")
        else:
            print("Ожидание присвоения партиций...")
            import time

            time.sleep(2)
            partitions = consumer.assignment()

        message_count = 0
        max_messages = 1000

        for message in consumer:
            if message_count >= max_messages:
                print(
                    f"Достигнут лимит сообщений ({max_messages}), останавливаем чтение"
                )
                break

            message_count += 1
            data = message.value

            try:
                msg_timestamp = datetime.fromisoformat(
                    data.get("timestamp", "").replace("Z", "+00:00")
                )
                messages.append(data)
                print(f"Сообщение #{message_count}: {msg_timestamp.isoformat()}")
            except (ValueError, TypeError) as e:
                print(f"Неверный формат timestamp в сообщении #{message_count}: {e}")
                messages.append(data)

        consumer.close()
        print(f"Прочитано сообщений из Kafka: {len(messages)}")

    except KafkaError as e:
        print(f"Ошибка Kafka: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при чтении из Kafka: {e}")

    return messages


def clean_data(raw_messages) -> pd.DataFrame:
    if not raw_messages:
        return pd.DataFrame()

    df = pd.json_normalize(raw_messages)

    df = df[
        [
            "timestamp",
            "city",
            "weather.current.temp_c",
            "weather.current.condition.text",
            "weather.current.humidity",
            "weather.current.wind_kph",
            "weather.current.pressure_mb",
            "weather.current.feelslike_c",
            "metadata.source",
        ]
    ]

    df.columns = [
        "timestamp",
        "city",
        "temperature_c",
        "condition_text",
        "humidity",
        "wind_kph",
        "pressure_mb",
        "feelslike_c",
        "source",
    ]

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

    df = df.dropna(subset=["timestamp", "city", "temperature_c", "humidity"])

    df = df[(df["temperature_c"].between(-100, 100)) & (df["humidity"].between(0, 100))]

    return df


def write_dataframe_to_sqlite(df) -> int:
    if df.empty:
        print("нет данных для записи")
        return 0

    conn = sqlite3.connect(get_db_path())

    df.to_sql(
        "cleaned_weather_data",
        conn,
        if_exists="append",
        index=False,
    )

    conn.close()

    print(f"Записано строк в БД: {len(df)}")
    return len(df)


def run_batch_processor():
    print("=" * 60)
    print("Запуск Hourly Batch Processor")
    print("=" * 60)

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw_weather_events")
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "weather_batch_processor")
    hours_back = int(os.getenv("BATCH_HOURS_BACK", "1"))

    print("⚙Настройки:")
    print(f"Kafka: {kafka_servers}")
    print(f"Топик: {topic}")
    print(f"Consumer Group: {consumer_group}")
    print(f"Период: последние {hours_back} час(ов)")
    print()

    try:
        init_database()
    except Exception as e:
        print(f"Ошибка инициализации базы данных: {e}")
        return

    raw_messages = read_from_kafka(
        kafka_servers=kafka_servers,
        topic=topic,
        consumer_group=consumer_group,
        hours_back=hours_back,
    )

    if not raw_messages:
        print("Нет новых сообщений для обработки")
        return

    df = clean_data(raw_messages)

    if df.empty:
        print("Нет валидных записей после очистки")
        return

    inserted = write_dataframe_to_sqlite(df)

    print("\n" + "=" * 60)
    print("Batch processing завершен!")
    print(f"Всего обработано: {len(raw_messages)} сообщений")
    print(f"Записано в БД: {inserted} записей")
    print(f"База данных: {get_db_path()}")
    print("=" * 60)


if __name__ == "__main__":
    run_batch_processor()
