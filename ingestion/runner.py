from ingestion.coingecko import ingest_coingecko
from ingestion.csv_ingest import ingest_csv_prices
from ingestion.csv_quirky_ingest import ingest_csv_quirky
from ingestion.unify import unify_data

from services.etl_logger import start_run, end_run


def run_all():
    run_id = start_run(source="all")

    try:
        try:
            ingest_coingecko()
        except Exception as e:
            print(f"[WARN] CoinGecko skipped: {e}")

        ingest_csv_prices()
        ingest_csv_quirky()
        unify_data()

        end_run(run_id, status="success")

    except Exception as e:
        end_run(run_id, status="failure")
        print(f"[ERROR] ETL failure: {e}")


if __name__ == "__main__":
    run_all()
