from fastapi import APIRouter, Query, Depends
from sqlalchemy.sql import text
from core.database import SessionLocal
import time
import uuid

from api.rate_limiter import rate_limiter

router = APIRouter()


@router.get("/stats")
def get_stats():
    session = SessionLocal()

    total_runs = session.execute(
        text("SELECT COUNT(*) FROM etl_runs")
    ).scalar()

    last_success = session.execute(
        text("""
            SELECT MAX(finished_at)
            FROM etl_runs
            WHERE status = 'success'
        """)
    ).scalar()

    last_failure = session.execute(
        text("""
            SELECT MAX(finished_at)
            FROM etl_runs
            WHERE status = 'failure'
        """)
    ).scalar()

    total_records = session.execute(
        text("""
            SELECT COALESCE(SUM(records_processed), 0)
            FROM etl_runs
            WHERE status = 'success'
        """)
    ).scalar()

    last_run = session.execute(
        text("""
            SELECT run_id, source, status, started_at, finished_at, records_processed
            FROM etl_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)
    ).mappings().fetchone()

    session.close()

    return {
        "total_runs": total_runs,
        "total_records_processed": total_records,
        "last_success": last_success,
        "last_failure": last_failure,
        "last_run": dict(last_run) if last_run else None
    }


@router.get(
    "/data",
    dependencies=[Depends(rate_limiter)]  # ✅ RATE LIMIT APPLIED
)
def get_data(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    symbol: str | None = None,
    source: str | None = None
):
    start_time = time.time()
    request_id = str(uuid.uuid4())

    session = SessionLocal()

    query = """
        SELECT symbol, name, price_usd, market_cap, source, updated_at
        FROM coins
        WHERE 1=1
    """
    params = {}

    if symbol:
        query += " AND symbol = :symbol"
        params["symbol"] = symbol.upper()

    if source:
        query += " AND source = :source"
        params["source"] = source

    query += " ORDER BY market_cap DESC NULLS LAST"
    query += " LIMIT :limit OFFSET :offset"

    params["limit"] = limit
    params["offset"] = offset

    rows = session.execute(
        text(query),
        params
    ).mappings().fetchall()

    session.close()

    latency_ms = int((time.time() - start_time) * 1000)

    return {
        "request_id": request_id,
        "api_latency_ms": latency_ms,
        "count": len(rows),
        "data": rows
    }
