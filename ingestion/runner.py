from ingestion.coingecko import ingest_coingecko
from ingestion.csv_ingest import ingest_csv_prices
from ingestion.csv_quirky_ingest import ingest_csv_quirky
from ingestion.unify import unify_data
from services.etl_logger import start_run, end_run


def run_all():
    run_id = start_run("all_sources")

    try:
        # API source (may rate-limit)
        try:
            ingest_coingecko()
        except Exception as e:
            print(f"[WARN] CoinGecko skipped: {e}")

        # CSV sources
        ingest_csv_prices()
        ingest_csv_quirky()

        # Unification
        unify_data()

        end_run(run_id, "success")

    except Exception as e:
        end_run(run_id, "failure")
        print(f"[ERROR] ETL failure: {e}")


if __name__ == "__main__":
    run_all()
