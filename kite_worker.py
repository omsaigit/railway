import requests
import time
import psycopg2
from datetime import datetime, time as dt_time
import os

# Config from environment variables
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
INSTRUMENT_CODE = os.getenv("INSTRUMENT_CODE", "NSE:INFY")  # default fallback
DB_URL = os.getenv("DATABASE_URL")

KITE_API_URL = f"https://api.kite.trade/quote?i={INSTRUMENT_CODE}"

# PostgreSQL DB connection
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS quotes (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL
)
''')
conn.commit()

def is_trading_time():
    now = datetime.now().time()
    return dt_time(9, 15) <= now <= dt_time(15, 30)

def fetch_and_store_quote():
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {API_KEY}:{ACCESS_TOKEN}"
    }

    try:
        response = requests.get(KITE_API_URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            cursor.execute(
                "INSERT INTO quotes (timestamp, data) VALUES (%s, %s)",
                (datetime.utcnow(), data)
            )
            conn.commit()
            print("Stored quote at", datetime.utcnow())
        else:
            print("API error:", response.status_code, response.text)
    except Exception as e:
        print("Request failed:", str(e))

if __name__ == "__main__":
    while True:
        if is_trading_time():
            fetch_and_store_quote()
        time.sleep(1)
