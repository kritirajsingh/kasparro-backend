import uuid
from sqlalchemy.sql import text
from core.database import SessionLocal


def log_etl_run(source: str, status: str, records: int = 0):
    session = SessionLocal()
    try:
        session.execute(
            text("""
                INSERT INTO etl_runs (
                    run_id,
                    source,
                    status,
                    records_processed,
                    finished_at
                )
                VALUES (
                    :run_id,
                    :source,
                    :status,
                    :records,
                    now()
                )
            """),
            {
                "run_id": str(uuid.uuid4()),
                "source": source,
                "status": status,
                "records": records
            }
        )
        session.commit()
    finally:
        session.close()
