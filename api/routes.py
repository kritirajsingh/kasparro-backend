# api/routes.py
from fastapi import APIRouter
from sqlalchemy.sql import text
from core.database import SessionLocal

router = APIRouter()   # ✅ THIS WAS MISSING

@router.get("/stats")
def get_stats():
    session = SessionLocal()
    try:
        total_runs = session.execute(
            text("SELECT COUNT(*) FROM etl_runs")
        ).scalar()

        total_coins = session.execute(
            text("SELECT COUNT(*) FROM coins")
        ).scalar()

        return {
            "etl_runs": total_runs,
            "coins": total_coins
        }
    finally:
        session.close()


@router.get("/data")
def get_data(limit: int = 20):
    session = SessionLocal()
    try:
        rows = session.execute(
            text("""
                SELECT symbol, name, price_usd, market_cap, source
                FROM coins
                ORDER BY market_cap DESC
                LIMIT :limit
            """),
            {"limit": limit}
        ).mappings().all()

        return list(rows)
    finally:
        session.close()
