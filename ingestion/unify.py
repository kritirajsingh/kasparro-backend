from sqlalchemy.sql import text
from core.database import SessionLocal

def unify_data():
    session = SessionLocal()

    # CoinGecko
    session.execute(text("""
        INSERT INTO coins (symbol, name, price_usd, market_cap, source)
        SELECT DISTINCT ON (LOWER(payload->>'symbol'))
            LOWER(payload->>'symbol'),
            payload->>'name',
            (payload->>'current_price')::numeric,
            (payload->>'market_cap')::numeric,
            'coingecko'
        FROM raw_coingecko
        ORDER BY LOWER(payload->>'symbol'), ingested_at DESC
        ON CONFLICT (symbol, source)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap,
            updated_at = now();
    """))

    # CoinPaprika
    session.execute(text("""
        INSERT INTO coins (symbol, name, price_usd, market_cap, source)
        SELECT DISTINCT ON (LOWER(payload->>'symbol'))
            LOWER(payload->>'symbol'),
            payload->>'name',
            (payload->'quotes'->'USD'->>'price')::numeric,
            (payload->'quotes'->'USD'->>'market_cap')::numeric,
            'coinpaprika'
        FROM raw_coinpaprika
        ORDER BY LOWER(payload->>'symbol'), ingested_at DESC
        ON CONFLICT (symbol, source)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap,
            updated_at = now();
    """))

    # CSV (clean)
    session.execute(text("""
        INSERT INTO coins (symbol, name, price_usd, market_cap, source)
        SELECT DISTINCT ON (LOWER(symbol))
            LOWER(symbol),
            name,
            price_usd::numeric,
            market_cap::numeric,
            'csv'
        FROM raw_csv_prices
        ORDER BY LOWER(symbol), ingested_at DESC
        ON CONFLICT (symbol, source)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap,
            updated_at = now();
    """))

    # CSV (quirky)
    session.execute(text("""
        INSERT INTO coins (symbol, name, price_usd, market_cap, source)
        SELECT DISTINCT ON (LOWER(ticker))
            LOWER(ticker),
            coin_name,
            NULLIF(REGEXP_REPLACE(usd_price, '[^0-9.]', '', 'g'), '')::numeric,
            NULLIF(REGEXP_REPLACE(cap, '[^0-9.]', '', 'g'), '')::numeric,
            'csv_quirky'
        FROM raw_csv_quirky
        ORDER BY LOWER(ticker), ingested_at DESC
        ON CONFLICT (symbol, source)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap,
            updated_at = now();
    """))

    session.commit()
    session.close()
