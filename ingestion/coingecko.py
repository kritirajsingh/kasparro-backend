import requests, json
from sqlalchemy.sql import text
from core.database import SessionLocal
from core.config import settings

def ingest_coingecko():
    params = {"vs_currency": "usd", "per_page": 50, "page": 1}
    r = requests.get(settings.COINGECKO_URL, params=params)
    r.raise_for_status()

    session = SessionLocal()
    for coin in r.json():
        session.execute(
            text("""
                INSERT INTO raw_coingecko (external_id, payload)
                VALUES (:id, :payload)
            """),
            {"id": coin["id"], "payload": json.dumps(coin)}
        )
    session.commit()
    session.close()
