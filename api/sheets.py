from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, HttpUrl
from typing import Optional
from config.database import get_connection

router = APIRouter()

# ------------------- SCHEMAS -------------------
class SheetCreate(BaseModel):
    google_sheet_url: str
    worksheet_name: str
    # cron_schedule: str
    status: bool = True
    start_time: Optional[str] = None  # New field
    end_time: Optional[str] = None    # New field
class SheetUpdate(BaseModel):
    google_sheet_url: Optional[str]
    worksheet_name: Optional[str]
    # cron_schedule: Optional[str]
    status: Optional[bool]
    start_time: Optional[str] = None  # New field
    end_time: Optional[str] = None    # New field

class SheetStatusUpdate(BaseModel):
    status: bool


# ------------------- CREATE -------------------
@router.post("/sheets")
def create_sheet(data: SheetCreate):
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO sheets (google_sheet_url, worksheet_name, status, start_time, end_time)
            VALUES (?, ?, ?, ?, ?)
        """, (
            str(data.google_sheet_url),
            data.worksheet_name,
            # data.cron_schedule,
            data.status,
            data.start_time,  
            data.end_time   
        ))
        conn.commit()

    return {"id": cursor.lastrowid, "message": "Sheet added successfully"}


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
    fields = []
    values = []

    for key, value in data.dict(exclude_none=True).items():
        fields.append(f"{key}=?")
        values.append(value)

    if not fields:
        raise HTTPException(400, "Nothing to update")

    values.append(sheet_id)

    with get_connection() as conn:
        conn.execute(
            f"UPDATE sheets SET {', '.join(fields)} WHERE id=?",
            values
        )
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