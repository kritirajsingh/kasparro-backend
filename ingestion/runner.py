from ingestion.coingecko import ingest_coingecko
from ingestion.csv_ingest import ingest_csv_prices
from ingestion.csv_quirky import ingest_csv_quirky
from ingestion.unify import unify_data


def run_all():
    try:
        ingest_coingecko()
    except Exception as e:
        print(f"[WARN] CoinGecko skipped: {e}")

    try:
        ingest_csv_prices()
        ingest_csv_quirky()
        unify_data()
    except Exception as e:
        print(f"[ERROR] ETL failure: {e}")
