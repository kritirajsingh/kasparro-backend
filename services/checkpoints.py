from sqlalchemy.sql import text
from core.database import SessionLocal


def get_last_checkpoint(source: str) -> int:
    session = SessionLocal()

    result = session.execute(
        text("""
            SELECT last_processed_at
            FROM etl_checkpoints
            WHERE source = :source
        """),
        {"source": source}
    ).fetchone()

    session.close()
    return result[0] if result else None


def update_checkpoint(source: str, last_processed_at):
    session = SessionLocal()

    session.execute(
        text("""
            INSERT INTO etl_checkpoints (source, last_processed_at)
            VALUES (:source, :last_processed_at)
            ON CONFLICT (source)
            DO UPDATE SET
                last_processed_at = EXCLUDED.last_processed_at,
                updated_at = NOW()
        """),
        {
            "source": source,
            "last_processed_at": last_processed_at
        }
    )

    session.commit()
    session.close()
