import uuid
from sqlalchemy.sql import text
from core.database import SessionLocal


def ensure_etl_table():
    session = SessionLocal()
    session.execute(
        text("""
        CREATE TABLE IF NOT EXISTS etl_runs (
            id SERIAL PRIMARY KEY,
            run_id UUID UNIQUE NOT NULL,
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMP DEFAULT now(),
            finished_at TIMESTAMP
        )
        """)
    )
    session.commit()
    session.close()


def start_run(source: str) -> str:
    ensure_etl_table()

    run_id = str(uuid.uuid4())
    session = SessionLocal()

    session.execute(
        text("""
            INSERT INTO etl_runs (run_id, source, status)
            VALUES (:run_id, :source, 'running')
        """),
        {"run_id": run_id, "source": source}
    )

    session.commit()
    session.close()
    return run_id


def end_run(run_id: str, status: str):
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
