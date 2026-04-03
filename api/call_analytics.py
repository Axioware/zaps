from fastapi import APIRouter
from config.database import get_connection

router = APIRouter()

@router.get("/analytics")
def get_call_analytics():
    with get_connection() as conn:

        # ---------- SUMMARY ----------
        total_calls = conn.execute("SELECT COUNT(*) FROM call_logs").fetchone()[0]

        answered = conn.execute("""
            SELECT COUNT(*) FROM call_logs
            WHERE call_disposition = 'Answered'
        """).fetchone()[0]

        unanswered = conn.execute("""
            SELECT COUNT(*) FROM call_logs
            WHERE call_disposition != 'Answered'
        """).fetchone()[0]

        transferred = conn.execute("""
            SELECT COUNT(*) FROM call_logs
            WHERE transfer_used = 'yes'
        """).fetchone()[0]

        wrong_numbers = conn.execute("""
            SELECT COUNT(*) FROM call_logs
            WHERE wrong_call = 'yes'
        """).fetchone()[0]

        avg_duration = conn.execute("""
            SELECT AVG(duration_secs) FROM call_logs
        """).fetchone()[0] or 0

        # ---------- TREND (GROUP BY DATE) ----------
        trend_rows = conn.execute("""
            SELECT 
                DATE(called_at) as date,
                COUNT(*) as made,
                SUM(CASE WHEN call_disposition = 'Answered' THEN 1 ELSE 0 END) as answered,
                SUM(CASE WHEN call_disposition != 'Answered' THEN 1 ELSE 0 END) as unanswered
            FROM call_logs
            GROUP BY DATE(called_at)
            ORDER BY DATE(called_at)
            LIMIT 7
        """).fetchall()

        trend_data = [
            {
                "date": row["date"],
                "made": row["made"],
                "answered": row["answered"],
                "unanswered": row["unanswered"],
            }
            for row in trend_rows
        ]

        # ---------- CATEGORY ----------
        category_data = [
            {"name": "Transferred", "value": transferred},
            {"name": "Wrong Number", "value": wrong_numbers},
        ]

        return {
            "summary": {
                "total_calls": total_calls,
                "answered": answered,
                "unanswered": unanswered,
                "transferred": transferred,
                "wrong_numbers": wrong_numbers,
                "avg_duration": round(avg_duration, 2),
            },
            "trend": trend_data,
            "category": category_data,
        }