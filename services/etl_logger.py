import uuid
from sqlalchemy.sql import text
from core.database import SessionLocal


def start_run(source: str) -> str:
    """
    Create a new ETL run entry and return run_id
    """
    run_id = str(uuid.uuid4())
    session = SessionLocal()

    session.execute(
        text("""
            INSERT INTO etl_runs (run_id, source, status)
            VALUES (:run_id, :source, 'success')
        """),
        {"run_id": run_id, "source": source}
    )

    session.commit()
    session.close()
    return run_id


def end_run(run_id: str, status: str):
    """
    Mark ETL run as success or failure
    """
    session = SessionLocal()

    session.execute(
        text("""
            UPDATE etl_runs
            SET status = :status,
                finished_at = now()
            WHERE run_id = :run_id
        """),
        {"run_id": run_id, "status": status}
    )

    session.commit()
    session.close()
