from sqlalchemy import text
from core.database import SessionLocal


def init_db():
    session = SessionLocal()
    try:
        # Coins table
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS coins (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT,
            price_usd NUMERIC,
            market_cap NUMERIC,
            source TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT now(),
            UNIQUE (symbol, source)
        );
        """))

        # Raw CoinGecko table
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_coingecko (
            id SERIAL PRIMARY KEY,
            payload JSONB,
            ingested_at TIMESTAMP DEFAULT now()
        );
        """))

        # Raw CSV prices
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_csv_prices (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            price_usd NUMERIC,
            market_cap NUMERIC,
            ingested_at TIMESTAMP DEFAULT now()
        );
        """))

        # Raw quirky CSV
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_csv_quirky (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            coin_name TEXT,
            usd_price TEXT,
            cap TEXT,
            ingested_at TIMESTAMP DEFAULT now()
        );
        """))

        # ETL run tracking
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            run_id TEXT PRIMARY KEY,
            source TEXT,
            status TEXT,
            started_at TIMESTAMP DEFAULT now(),
            finished_at TIMESTAMP
        );
        """))

        session.commit()
        print("[DB INIT] Tables verified/created successfully")

    except Exception as e:
        session.rollback()
        print(f"[DB INIT ERROR] {e}")
        raise
    finally:
        session.close()
