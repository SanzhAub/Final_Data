import sqlite3

from database import get_db_path


def run_daily_analytics():
    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_weather_summary (
            date TEXT NOT NULL,
            city TEXT NOT NULL,
            min_temp REAL,
            max_temp REAL,
            avg_temp REAL,
            avg_humidity REAL,
            records_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, city)
        )
        """
    )

    cursor.execute(
        """
        INSERT OR REPLACE INTO daily_weather_summary (
            date,
            city,
            min_temp,
            max_temp,
            avg_temp,
            avg_humidity,
            records_count
        )
        SELECT
            DATE(timestamp),
            city,
            MIN(temperature_c),
            MAX(temperature_c),
            AVG(temperature_c),
            AVG(humidity),
            COUNT(*)
        FROM cleaned_weather_data
        WHERE DATE(timestamp) = DATE('now')
        GROUP BY DATE(timestamp), city
        """
    )
    # WHERE DATE(timestamp) = DATE('now', '-1 day')

    conn.commit()
    conn.close()

    print("Daily analytics completed")
