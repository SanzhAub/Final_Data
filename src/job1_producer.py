import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import os


def run_producer():
    """Основная функция для сбора данных"""
    print("Запуск продюсера данных о погоде...")

    # Получаем настройки из переменных окружения
    api_key = os.getenv("WEATHERAPI_KEY")
    location = os.getenv("WEATHER_LOCATION", "Almaty")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw_weather_events")
    interval = int(os.getenv("PRODUCER_INTERVAL", "30"))

    if not api_key or api_key == "ваш_настоящий_api_ключ_здесь":
        print("Ошибка: WEATHERAPI_KEY не установлен!")
        print("   Установите ключ в .env файле: WEATHERAPI_KEY=ваш_ключ")
        return

    print(f"Локация: {location}")
    print(f"Kafka: {kafka_servers}")
    print(f"Топик: {topic}")
    print(f"Интервал: {interval} секунд")

    # Создаем продюсера
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\nИтерация #{iteration}")

            try:
                # Получаем данные о погоде
                url = "http://api.weatherapi.com/v1/current.json"
                params = {"key": api_key, "q": location, "aqi": "no"}

                response = requests.get(url, params=params, timeout=10)
                weather_data = response.json()

                # Формируем сообщение для Kafka
                message = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "city": location,
                    "weather": weather_data,
                    "metadata": {"source": "weatherapi.com", "iteration": iteration},
                }

                # Отправляем в Kafka
                producer.send(topic, message)
                producer.flush()

                # Выводим информацию
                temp = weather_data["current"]["temp_c"]
                condition = weather_data["current"]["condition"]["text"]
                print(f"Отправлено: {temp}°C, {condition}")

            except Exception as e:
                print(f"Ошибка: {e}")

            # Ждем перед следующей итерацией
            if iteration < 10:  # Для демонстрации - 10 итераций
                print(f"⏳ Ожидание {interval} секунд...")
                time.sleep(interval)
            else:
                print("Завершено 10 итераций")
                break

    except KeyboardInterrupt:
        print("\nОстановка по запросу пользователя")
    finally:
        producer.close()
        print("Продюсер остановлен")


if __name__ == "__main__":
    # Для локального тестирования
    run_producer()
