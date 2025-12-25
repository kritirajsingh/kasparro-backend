CREATE TABLE IF NOT EXISTS raw_coingecko (
    id SERIAL PRIMARY KEY,
    external_id TEXT,
    payload JSONB,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_coinpaprika (
    id SERIAL PRIMARY KEY,
    external_id TEXT,
    payload JSONB,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_csv_prices (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    price_usd TEXT,
    market_cap TEXT,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_csv_quirky (
    id SERIAL PRIMARY KEY,
    ticker TEXT,
    coin_name TEXT,
    usd_price TEXT,
    cap TEXT,
    ingested_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS coins (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    price_usd NUMERIC,
    market_cap NUMERIC,
    source TEXT,
    updated_at TIMESTAMP DEFAULT now(),
    UNIQUE(symbol, source)
);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    records_processed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT now(),
    finished_at TIMESTAMP
);
