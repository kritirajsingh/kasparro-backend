import csv
import os
import re
from sqlalchemy.sql import text
from core.database import SessionLocal

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "coins_quirky.csv")

def clean_number(value: str) -> float:
    return float(re.sub(r"[^\d.]", "", value))

def ingest_csv_quirky():
    session = SessionLocal()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            session.execute(
                text("""
                    INSERT INTO raw_csv_quirky
                    (ticker, coin_name, usd_price, cap)
                    VALUES (:ticker, :coin_name, :usd_price, :cap)
                """),
                {
                    "ticker": row["ticker"],
                    "coin_name": row["coin_name"],
                    "usd_price": clean_number(row["usd_price"]),
                    "cap": clean_number(row["cap"])
                }
            )

    session.commit()
    session.close()
