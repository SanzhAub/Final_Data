import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import os


def run_producer():
    print("🚀 Weather producer started (continuous mode)")

    api_key = os.getenv("WEATHERAPI_KEY")
    location = os.getenv("WEATHER_LOCATION", "Almaty")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw_weather_events")
    interval = int(os.getenv("PRODUCER_INTERVAL", "60"))  # seconds

    if not api_key:
        raise RuntimeError("WEATHERAPI_KEY is not set")

    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"📍 Location: {location}")
    print(f"📨 Kafka topic: {topic}")
    print(f"⏱ Interval: {interval} seconds")

    try:
        while True:
            response = requests.get(
                "http://api.weatherapi.com/v1/current.json",
                params={"key": api_key, "q": location, "aqi": "no"},
                timeout=10,
            )
            response.raise_for_status()

            weather_data = response.json()

            message = {
                "timestamp": datetime.utcnow().isoformat(),
                "city": location,
                "weather": weather_data,
                "metadata": {"source": "weatherapi.com"},
            }

            producer.send(topic, message)
            producer.flush()

            temp = weather_data["current"]["temp_c"]
            condition = weather_data["current"]["condition"]["text"]
            print(f"✅ Sent: {temp}°C, {condition}")

            time.sleep(interval)

    except Exception as e:
        print(f"❌ Producer error: {e}")
        raise

    finally:
        producer.close()
        print("🛑 Producer stopped")
