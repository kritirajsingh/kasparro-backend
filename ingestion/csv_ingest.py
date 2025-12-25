import csv
import os
from sqlalchemy.sql import text
from core.database import SessionLocal

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "coins.csv")

def ingest_csv_prices():
    session = SessionLocal()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            session.execute(
                text("""
                    INSERT INTO raw_csv_prices (symbol, name, price_usd, market_cap)
                    VALUES (:symbol, :name, :price_usd, :market_cap)
                """),
                {
                    "symbol": row["symbol"],
                    "name": row["name"],
                    "price_usd": row["price_usd"],
                    "market_cap": row["market_cap"]
                }
            )

    session.commit()
    session.close()
