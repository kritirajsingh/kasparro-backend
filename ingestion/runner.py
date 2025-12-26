from core.db_init import init_tables
from services.etl_logger import start_run, end_run

def run_all():
    init_tables()   # 🔥 THIS FIXES EVERYTHING

    run_id = start_run("all_sources")
    try:
        ingest_coingecko()
        ingest_csv_prices()
        ingest_csv_quirky()
        unify_data()
        end_run(run_id, "success")
    except Exception as e:
        end_run(run_id, "failure")
        raise
