from ingestion.coingecko import ingest_coingecko
from ingestion.csv_ingest import ingest_csv_prices
from ingestion.csv_quirky_ingest import ingest_csv_quirky
from ingestion.unify import unify_data

from services.etl_logger import log_etl_run


def run_all():
    # -------- CoinGecko --------
    try:
        ingest_coingecko()
        log_etl_run(source="coingecko", status="success")
    except Exception as e:
        log_etl_run(source="coingecko", status="failure")
        print(f"[WARN] CoinGecko skipped: {e}")

    # -------- CSV + Unification --------
    try:
        ingest_csv_prices()
        log_etl_run(source="csv", status="success")

        ingest_csv_quirky()
        log_etl_run(source="csv_quirky", status="success")

        unify_data()
        log_etl_run(source="unify", status="success")

    except Exception as e:
        log_etl_run(source="etl_pipeline", status="failure")
        print(f"[ERROR] ETL failure: {e}")


if __name__ == "__main__":
    run_all()
