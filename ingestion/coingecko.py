import time
import json
import requests
from sqlalchemy.sql import text
from core.database import SessionLocal

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

MAX_RETRIES = 5
BASE_BACKOFF = 2  # seconds


def ingest_coingecko():
    params = {
        "vs_currency": "usd",
        "per_page": 50,
        "page": 1
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                COINGECKO_URL,
                params=params,
                timeout=15
            )

            if response.status_code == 429:
                raise requests.HTTPError("Rate limited (429)")

            response.raise_for_status()
            data = response.json()
            break

        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"[WARN] CoinGecko failed after {MAX_RETRIES} retries. Skipping.")
                return  # 🔥 DO NOT CRASH

            backoff = BASE_BACKOFF ** attempt
            print(f"[Retry] CoinGecko attempt {attempt} failed. Retrying in {backoff}s")
            time.sleep(backoff)

    session = SessionLocal()

    for coin in data:
        session.execute(
            text("""
                INSERT INTO raw_coingecko (external_id, payload)
                VALUES (:id, :payload)
                ON CONFLICT (external_id) DO NOTHING
            """),
            {
                "id": coin["id"],
                "payload": json.dumps(coin)
            }
        )

    session.commit()
    session.close()

    print("[OK] CoinGecko ingestion completed")
