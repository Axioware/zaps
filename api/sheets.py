from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from config.database import get_connection

router = APIRouter()

# ------------------- SCHEMAS -------------------
class SheetCreate(BaseModel):
    google_sheet_url: str
    worksheet_name: str
    # cron_schedule: str
    agent_id: str
    status: bool = True
    schedule: Dict[str, Dict[str, str]]  
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class SheetUpdate(BaseModel):
    google_sheet_url: Optional[str]
    worksheet_name: Optional[str]
    # cron_schedule: Optional[str]
    agent_id: Optional[str]
    status: Optional[bool]
    schedule: Optional[Dict[str, Dict[str, str]]]
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class SheetStatusUpdate(BaseModel):
    status: bool


# ------------------- CREATE -------------------
@router.post("/sheets")
def create_sheet(data: SheetCreate):
    last_id = None
    with get_connection() as conn:
        for day, times in data.schedule.items():
            start = times.get("start", "00:00")
            end = times.get("end", "00:00")

            # Skip inactive days
            if start == end == "00:00":
                continue

            cursor = conn.execute("""
                INSERT INTO sheets (google_sheet_url, worksheet_name, agent_id, status, day_of_week, start_time, end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                data.google_sheet_url,
                data.worksheet_name,
                data.agent_id,
                data.status,
                day,
                start,
                end
            ))
            last_id = cursor.lastrowid

        conn.commit()

    return {"id": last_id, "message": "Sheet(s) added successfully"}


# ------------------- GET -------------------
@router.get("/sheets")
def get_sheets(
    status: Optional[bool] = Query(None),
    limit: int = 10,
    offset: int = 0
):
    with get_connection() as conn:
        query = "SELECT * FROM sheets"
        params = []

        if status is not None:
            query += " WHERE status=?"
            params.append(status)

        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()

    return [dict(r) for r in rows]


# ------------------- UPDATE -------------------
@router.put("/sheets/{sheet_id}")
def update_sheet(sheet_id: int, data: SheetUpdate):
    with get_connection() as conn:
        # If schedule provided, update by deleting old days for this sheet and inserting new active days
        if data.schedule:
            # Delete old active day rows (keep original id if needed, else just remove)
            conn.execute("DELETE FROM sheets WHERE id=? OR id IN (SELECT id FROM sheets WHERE id=?)", (sheet_id, sheet_id))

            last_id = None
            for day, times in data.schedule.items():
                start = times.get("start", "00:00")
                end = times.get("end", "00:00")
                if start == end == "00:00":
                    continue
                cursor = conn.execute("""
                    INSERT INTO sheets (google_sheet_url, worksheet_name, agent_id, status, day_of_week, start_time, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    data.google_sheet_url or "",
                    data.worksheet_name or "",
                    data.agent_id or "",
                    data.status if data.status is not None else True,
                    day,
                    start,
                    end
                ))
                last_id = cursor.lastrowid
            conn.commit()
            return {"id": last_id, "message": "Sheet schedule updated"}

        # If no schedule, just update other fields
        fields = []
        values = []
        for key, value in data.dict(exclude_none=True, exclude={"schedule"}).items():
            fields.append(f"{key}=?")
            values.append(value)

        if not fields:
            raise HTTPException(400, "Nothing to update")

        values.append(sheet_id)
        conn.execute(f"UPDATE sheets SET {', '.join(fields)} WHERE id=?", values)
        conn.commit()

    return {"message": "Sheet updated"}


# ------------------- TOGGLE -------------------
@router.patch("/sheets/{sheet_id}/status")
def toggle_status(sheet_id: int, data: SheetStatusUpdate):
    with get_connection() as conn:
        conn.execute(
            "UPDATE sheets SET status=? WHERE id=?",
            (data.status, sheet_id)
        )
        conn.commit()

    return {"message": "Status updated"}


# ------------------- DELETE -------------------
@router.delete("/sheets/{sheet_id}")
def delete_sheet(sheet_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM sheets WHERE id=?", (sheet_id,))
        conn.commit()

    return {"message": "Sheet deleted"}