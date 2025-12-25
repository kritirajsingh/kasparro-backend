from ingestion.coingecko import ingest_coingecko
from ingestion.coinpaprika import ingest_coinpaprika
from ingestion.csv_ingest import ingest_csv_prices
from ingestion.csv_quirky_ingest import ingest_csv_quirky
from ingestion.unify import unify_data

def run_all():
    ingest_coingecko()
    ingest_coinpaprika()
    ingest_csv_prices()
    ingest_csv_quirky()
    unify_data()

if __name__ == "__main__":
    run_all()
