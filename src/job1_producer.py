import json
import requests
from kafka import KafkaProducer
from datetime import datetime
import os

def run_producer():
    print("Weather producer started (one iteration)")

    api_key = os.getenv("WEATHERAPI_KEY")
    location = os.getenv("WEATHER_LOCATION", "Almaty")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "raw_weather_events")

    if not api_key:
        raise RuntimeError("WEATHERAPI_KEY is not set")

    try:
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

        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        producer.close()

        temp = weather_data["current"]["temp_c"]
        condition = weather_data["current"]["condition"]["text"]
        print(f"Sent: {temp}°C, {condition}")

    except Exception as e:
        print(f"Producer error: {e}")
        raise
