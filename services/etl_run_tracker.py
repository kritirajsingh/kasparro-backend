import uuid
from datetime import datetime
from sqlalchemy.sql import text
from core.database import SessionLocal


def start_etl_run(source: str) -> str:
    run_id = str(uuid.uuid4())
    session = SessionLocal()

    session.execute(
        text("""
            INSERT INTO etl_runs (run_id, source, status, started_at)
            VALUES (:run_id, :source, 'running', :started_at)
        """),
        {
            "run_id": run_id,
            "source": source,
            "started_at": datetime.utcnow()
        }
    )

    session.commit()
    session.close()
    return run_id


def finish_etl_run(run_id: str, records: int, status: str, error: str = None):
    session = SessionLocal()

    session.execute(
        text("""
            UPDATE etl_runs
            SET
                records_processed = :records,
                status = :status,
                finished_at = :finished_at,
                error_message = :error
            WHERE run_id = :run_id
        """),
        {
            "run_id": run_id,
            "records": records,
            "status": status,
            "finished_at": datetime.utcnow(),
            "error": error
        }
    )

    session.commit()
    session.close()
