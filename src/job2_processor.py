import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Импортируем модуль базы данных
from database import get_db_path, init_database, insert_weather_record


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


def clean_data(raw_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_records = []
    skipped_count = 0

    print(f"\nОчистка и валидация {len(raw_messages)} сообщений...")

    for idx, msg in enumerate(raw_messages, 1):
        try:
            if not all(key in msg for key in ["timestamp", "city", "weather"]):
                print(f"Сообщение #{idx}: отсутствуют обязательные поля")
                skipped_count += 1
                continue

            if "current" not in msg["weather"]:
                print(f"Сообщение #{idx}: отсутствует weather.current")
                skipped_count += 1
                continue

            current = msg["weather"]["current"]

            required_fields = ["temp_c", "condition", "humidity"]
            if not all(field in current for field in required_fields):
                print(f"Сообщение #{idx}: отсутствуют обязательные поля в current")
                skipped_count += 1
                continue

            if "text" not in current["condition"]:
                print(f"Сообщение #{idx}: отсутствует condition.text")
                skipped_count += 1
                continue

            try:
                temp_c = float(current["temp_c"])
                humidity = int(current["humidity"])
                condition_text = str(current["condition"]["text"]).strip()

                if not (-100 <= temp_c <= 100):
                    print(f"Сообщение #{idx}: неверная температура: {temp_c}")
                    skipped_count += 1
                    continue

                if not (0 <= humidity <= 100):
                    print(f"Сообщение #{idx}: неверная влажность: {humidity}")
                    skipped_count += 1
                    continue

                if not condition_text:
                    print(f"Сообщение #{idx}: пустое условие погоды")
                    skipped_count += 1
                    continue

            except (ValueError, TypeError) as e:
                print(f"Сообщение #{idx}: ошибка валидации типов: {e}")
                skipped_count += 1
                continue

            timestamp = msg["timestamp"]
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp = dt.isoformat()
            except (ValueError, TypeError):
                pass

            wind_kph = current.get("wind_kph")
            if wind_kph is not None:
                try:
                    wind_kph = float(wind_kph)
                except (ValueError, TypeError):
                    wind_kph = None

            pressure_mb = current.get("pressure_mb")
            if pressure_mb is not None:
                try:
                    pressure_mb = float(pressure_mb)
                except (ValueError, TypeError):
                    pressure_mb = None

            feelslike_c = current.get("feelslike_c")
            if feelslike_c is not None:
                try:
                    feelslike_c = float(feelslike_c)
                except (ValueError, TypeError):
                    feelslike_c = None

            cleaned_record = {
                "timestamp": timestamp,
                "city": str(msg["city"]).strip(),
                "temperature_c": temp_c,
                "condition_text": condition_text,
                "humidity": humidity,
                "wind_kph": wind_kph,
                "pressure_mb": pressure_mb,
                "feelslike_c": feelslike_c,
                "source": msg.get("metadata", {}).get("source", "weatherapi.com"),
            }

            cleaned_records.append(cleaned_record)
            print(f"Сообщение #{idx}: очищено и валидировано")

        except Exception as e:
            print(f"Сообщение #{idx}: неожиданная ошибка при очистке: {e}")
            skipped_count += 1
            continue

    print(f"\nОчищено записей: {len(cleaned_records)}")
    print(f"Пропущено записей: {skipped_count}")

    return cleaned_records


def write_to_sqlite(cleaned_records: List[Dict[str, Any]]) -> Dict[str, int]:
    stats = {"inserted": 0, "duplicates": 0, "errors": 0}

    if not cleaned_records:
        print("Нет записей для записи в базу данных")
        return stats

    print(f"\nЗапись {len(cleaned_records)} записей в SQLite...")

    for record in cleaned_records:
        success = insert_weather_record(record)
        if success:
            stats["inserted"] += 1
        else:
            stats["duplicates"] += 1

    print("\nСтатистика записи:")
    print(f"Вставлено: {stats['inserted']}")
    print(f"Дубликаты: {stats['duplicates']}")
    print(f"Ошибки: {stats['errors']}")

    return stats


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

    cleaned_records = clean_data(raw_messages)

    if not cleaned_records:
        print("Нет валидных записей после очистки")
        return

    stats = write_to_sqlite(cleaned_records)

    print("\n" + "=" * 60)
    print("Batch processing завершен!")
    print(f"Всего обработано: {len(raw_messages)} сообщений")
    print(f"Записано в БД: {stats['inserted']} записей")
    print(f"База данных: {get_db_path()}")
    print("=" * 60)


if __name__ == "__main__":
    run_batch_processor()
