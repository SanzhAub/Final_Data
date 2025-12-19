from kafka import KafkaConsumer
import json

print("📡 Подключаемся к Kafka...")

consumer = KafkaConsumer(
    "raw_weather_events",
    bootstrap_servers="localhost:9093",  # 9093 - порт с хоста
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",  # читать с самого начала
    consumer_timeout_ms=10000,  # таймаут 10 секунд
)

print("✅ Подключено! Слушаем сообщения...")

count = 0
try:
    for message in consumer:
        count += 1
        data = message.value
        print(f"\n📨 Сообщение #{count}:")
        print(f"   🕒 Время: {data['timestamp']}")
        print(f"   📍 Город: {data['city']}")
        print(f"   🌡 Температура: {data['weather']['current']['temp_c']}°C")
        print(f"   🌤 Состояние: {data['weather']['current']['condition']['text']}")
        print(f"   💧 Влажность: {data['weather']['current']['humidity']}%")

except Exception as e:
    print(f"❌ Ошибка: {e}")

print(f"\n🎯 Всего получено сообщений: {count}")
consumer.close()
