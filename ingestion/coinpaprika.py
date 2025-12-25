import requests, json
from sqlalchemy.sql import text
from core.database import SessionLocal
from core.config import settings

def ingest_coinpaprika():
    r = requests.get(settings.COINPAPRIKA_URL, timeout=30)
    r.raise_for_status()

    session = SessionLocal()
    for coin in r.json():
        session.execute(
            text("""
                INSERT INTO raw_coinpaprika (external_id, payload)
                VALUES (:id, :payload)
            """),
            {"id": coin["id"], "payload": json.dumps(coin)}
        )
    session.commit()
    session.close()
