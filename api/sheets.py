from os import times

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import Dict, Optional
from config.database import get_connection

router = APIRouter()

# ----------- INNER MODEL -----------
class DaySchedule(BaseModel):
    start: str
    end: str


# ----------- CREATE -----------
class SheetCreate(BaseModel):
    google_sheet_url: str
    worksheet_name: str
    agent_id: str
    status: bool = True
    schedule: Dict[str, DaySchedule]


# ----------- UPDATE -----------
class SheetUpdate(BaseModel):
    google_sheet_url: Optional[str] = None
    worksheet_name: Optional[str] = None
    agent_id: Optional[str] = None
    status: Optional[bool] = None
    schedule: Optional[Dict[str, DaySchedule]] = None


class SheetStatusUpdate(BaseModel):
    status: bool


# ------------------- CREATE -------------------
@router.post("/sheets")
def create_sheet(data: SheetCreate):
    last_id = None
    with get_connection() as conn:
        # Insert main sheet info
        cursor = conn.execute("""
            INSERT INTO sheets (google_sheet_url, worksheet_name, agent_id, status)
            VALUES (?, ?, ?, ?)
        """, (
            data.google_sheet_url,
            data.worksheet_name,
            data.agent_id,
            data.status,
        ))
        sheet_id = cursor.lastrowid

        # Insert schedule into sheet_schedules
        for day, times in data.schedule.items():
            start = times.start
            end = times.end
            if start == end == "00:00":
                continue
            conn.execute("""
                INSERT INTO sheet_schedules (sheet_id, day_of_week, start_time, end_time)
                VALUES (?, ?, ?, ?)
            """, (sheet_id, day, start, end))

        conn.commit()

    return {"id": sheet_id, "message": "Sheet added successfully"}


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
        sheets = [dict(r) for r in rows]

        # Attach schedules
        for sheet in sheets:
            schedule_rows = conn.execute(
                "SELECT day_of_week, start_time, end_time FROM sheet_schedules WHERE sheet_id=?",
                (sheet["id"],)
            ).fetchall()
            sheet["schedule"] = {r["day_of_week"]: {"start": r["start_time"], "end": r["end_time"]} for r in schedule_rows}

    return sheets


# ------------------- UPDATE -------------------
@router.put("/sheets/{sheet_id}")
def update_sheet(sheet_id: int, data: SheetUpdate):
    with get_connection() as conn:
        # Update main sheet info
        fields = []
        values = []
        for key, value in data.dict(exclude_none=True, exclude={"schedule"}).items():
            fields.append(f"{key}=?")
            values.append(value)

        if fields:
            values.append(sheet_id)
            conn.execute(f"UPDATE sheets SET {', '.join(fields)} WHERE id=?", values)

        # Update schedule if provided
        if data.schedule:
            # Delete old schedules for this sheet
            conn.execute("DELETE FROM sheet_schedules WHERE sheet_id=?", (sheet_id,))

            # Insert new schedules
            for day, times in data.schedule.items():
                start = times.start
                end = times.end
                if start == end == "00:00":
                    continue
                conn.execute("""
                    INSERT INTO sheet_schedules (sheet_id, day_of_week, start_time, end_time)
                    VALUES (?, ?, ?, ?)
                """, (sheet_id, day, start, end))

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
        conn.execute("DELETE FROM sheet_schedules WHERE sheet_id=?", (sheet_id,))
        conn.execute("DELETE FROM sheets WHERE id=?", (sheet_id,))
        conn.commit()
    return {"message": "Sheet deleted"}