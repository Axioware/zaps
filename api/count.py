from fastapi import APIRouter
from config.database import get_connection

router = APIRouter()

@router.get("/sheets/stats")
def get_sheet_stats():
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 
                SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS active,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS inactive,
                COUNT(*) AS total
            FROM sheets
        """)

        row = cursor.fetchone()

        return {
            "active": row[0] or 0,
            "inactive": row[1] or 0,
            "total": row[2] or 0
        }