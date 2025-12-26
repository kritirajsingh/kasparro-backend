from sqlalchemy import text
from fastapi import HTTPException

@router.get("/stats")
def get_stats():
    try:
        total_runs = session.execute(
            text("SELECT COUNT(*) FROM etl_runs")
        ).scalar()
        return {"total_etl_runs": total_runs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
