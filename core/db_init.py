from sqlalchemy import text
from core.database import engine


def init_tables():
    """
    Creates required tables if they do not exist.
    Safe to run multiple times.
    """
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS coins (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            price_usd NUMERIC,
            market_cap NUMERIC,
            source TEXT,
            updated_at TIMESTAMP DEFAULT now(),
            UNIQUE(symbol, source)
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_csv_prices (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            price_usd NUMERIC,
            market_cap NUMERIC,
            ingested_at TIMESTAMP DEFAULT now()
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS raw_csv_quirky (
            id SERIAL PRIMARY KEY,
            ticker TEXT,
            coin_name TEXT,
            usd_price TEXT,
            cap TEXT,
            ingested_at TIMESTAMP DEFAULT now()
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            run_id UUID PRIMARY KEY,
            source TEXT NOT NULL,
            status TEXT CHECK (status IN ('success','failure')),
            records_processed INTEGER DEFAULT 0,
            started_at TIMESTAMP DEFAULT now(),
            finished_at TIMESTAMP
        );
        """))
