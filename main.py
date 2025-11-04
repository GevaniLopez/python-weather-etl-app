import datetime as dt
import sqlite3
import requests
from typing import List, Dict

DB_PATH = "weather.db"

# Pensacola, FL and fixed date (Sep 26)
LAT, LON = 30.4213, -87.2169
MONTH, DAY = 9, 26
YEARS_BACK = 5  # last 5 years (excluding current year if not complete)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

def target_dates(years_back: int) -> List[dt.date]:
    today = dt.date.today()
    current_year = today.year
    dates = []
    for y in range(current_year - years_back, current_year):
        dates.append(dt.date(y, MONTH, DAY))
    return dates

def fetch_hourly(date: dt.date) -> Dict:
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": date.isoformat(),
        "end_date": date.isoformat(),
        "hourly": ["temperature_2m", "precipitation", "windspeed_10m"],
        "timezone": "UTC",
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS weather (
            dt        TEXT NOT NULL,
            hour      INTEGER NOT NULL,
            temp_c    REAL,
            precip_mm REAL,
            wind_ms   REAL,
            PRIMARY KEY (dt, hour)
        )
        '''
    )
    conn.commit()

def upsert_day(conn: sqlite3.Connection, payload: Dict, date: dt.date) -> int:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", []) or []
    temps = hourly.get("temperature_2m", []) or []
    precs = hourly.get("precipitation", []) or []
    winds = hourly.get("windspeed_10m", []) or []
    inserted = 0
    for i, ts in enumerate(times):
        try:
            hour = int(ts.split('T')[1].split(':')[0]) if 'T' in ts else i
        except Exception:
            hour = i
        conn.execute(
            '''
            INSERT OR REPLACE INTO weather (dt, hour, temp_c, precip_mm, wind_ms)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (date.isoformat(),
             hour,
             float(temps[i]) if i < len(temps) else None,
             float(precs[i]) if i < len(precs) else None,
             float(winds[i]) if i < len(winds) else None)
        )
        inserted += 1
    conn.commit()
    return inserted

def summary(conn: sqlite3.Connection) -> Dict[str, float]:
    row = conn.execute(
        '''
        SELECT
            COUNT(*) AS n_rows,
            AVG(temp_c) AS avg_temp_c,
            SUM(precip_mm) AS total_precip_mm,
            AVG(wind_ms) AS avg_wind_ms
        FROM weather
        '''
    ).fetchone()
    return {
        "rows": row[0] or 0,
        "avg_temp_c": round(row[1], 2) if row[1] is not None else None,
        "total_precip_mm": round(row[2], 2) if row[2] is not None else None,
        "avg_wind_ms": round(row[3], 2) if row[3] is not None else None,
    }

def main():
    dates = target_dates(YEARS_BACK)
    conn = sqlite3.connect(DB_PATH)
    ensure_table(conn)

    total = 0
    for d in dates:
        try:
            data = fetch_hourly(d)
            inserted = upsert_day(conn, data, d)
            total += inserted
            print(f"{d.isoformat()}: inserted {inserted} hourly records")
        except requests.HTTPError as e:
            print(f"{d.isoformat()}: HTTP error {e}")
        except Exception as e:
            print(f"{d.isoformat()}: skipped due to {e}")

    stats = summary(conn)
    print("\nSummary across all dates:")
    print(f"Rows: {stats['rows']}")
    print(f"Avg Temp (°C): {stats['avg_temp_c']}")
    print(f"Total Precip (mm): {stats['total_precip_mm']}")
    print(f"Avg Wind (m/s): {stats['avg_wind_ms']}")

if __name__ == "__main__":
    main()