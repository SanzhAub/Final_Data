import os
import sqlite3
from typing import Any, Dict, Optional


def get_db_path() -> str:
    data_dir = os.getenv("DATA_DIR", "/opt/airflow/data")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "weather.db")


def get_db_connection():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cleaned_weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            city TEXT NOT NULL,
            temperature_c REAL NOT NULL,
            condition_text TEXT NOT NULL,
            humidity INTEGER NOT NULL,
            wind_kph REAL,
            pressure_mb REAL,
            feelslike_c REAL,
            source TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(timestamp, city)
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp ON cleaned_weather_data(timestamp)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_city ON cleaned_weather_data(city)
    """)

    conn.commit()
    conn.close()
    print(f"✅ База данных инициализирована: {get_db_path()}")


def insert_weather_record(record: Dict[str, Any]) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO cleaned_weather_data
            (timestamp, city, temperature_c, condition_text, humidity,
             wind_kph, pressure_mb, feelslike_c, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                record["timestamp"],
                record["city"],
                record["temperature_c"],
                record["condition_text"],
                record["humidity"],
                record.get("wind_kph"),
                record.get("pressure_mb"),
                record.get("feelslike_c"),
                record.get("source", "weatherapi.com"),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при вставке записи: {e}")
        return False
    finally:
        conn.close()


def get_last_processed_timestamp(city: str = "Almaty") -> Optional[str]:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT MAX(timestamp) as last_timestamp
            FROM cleaned_weather_data
            WHERE city = ?
        """,
            (city,),
        )

        result = cursor.fetchone()
        return result["last_timestamp"] if result and result["last_timestamp"] else None
    except Exception as e:
        print(f"❌ Ошибка при получении последнего timestamp: {e}")
        return None
    finally:
        conn.close()


def get_record_count() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) as count FROM cleaned_weather_data")
        result = cursor.fetchone()
        return result["count"] if result else 0
    except Exception as e:
        print(f"❌ Ошибка при подсчете записей: {e}")
        return 0
    finally:
        conn.close()
