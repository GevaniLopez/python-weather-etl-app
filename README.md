# Python Weather ETL App

Fetch historical hourly weather for Pensacola, FL on **September 26** for the last 5 years, store into SQLite, and print quick summary stats.

## Stack
- Python 3.10+
- Requests (HTTP client)
- SQLite (built-in)
- Minimal standard library (no framework)

## Quickstart

```bash
# 1) Create & activate a virtual environment
python -m venv .venv
# Windows:
# .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Run
python main.py
```

This creates a local `weather.db` (SQLite) and prints summary metrics.

## What it does
- Builds a list of target dates (09-26 for each of the last 5 years)
- Calls Open‑Meteo archive API for hourly temperature, precipitation, and wind
- Upserts into a simple `weather` table keyed by `(date, hour)`
- Prints: row count, average temperature, total precipitation, average wind

## Files
- `main.py` — single-file ETL script
- `requirements.txt` — pinned dependencies
- `.gitignore` — ignores `weather.db`, venv cache, and artifacts

## Example output
```
2021-09-26: inserted 24 hourly records
2022-09-26: inserted 24 hourly records
2023-09-26: inserted 24 hourly records
2024-09-26: inserted 24 hourly records
Summary across all dates:
Rows: 96
Avg Temp (°C): 26.41
Total Precip (mm): 3.2
Avg Wind (m/s): 4.18
```

## Notes
- API: https://archive-api.open-meteo.com/v1/archive (no API key required)
- Location: Pensacola, FL (lat 30.4213, lon -87.2169)
- Timezone: UTC for simplicity
- You can change `YEARS_BACK`, `LAT`, `LON`, `MONTH`, `DAY` at the top of `main.py`.