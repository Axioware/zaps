from fastapi import APIRouter, Query
from schemas.sheets import SheetCreate, SheetUpdate, SheetStatusUpdate
from services.sheets_service import *

router = APIRouter()

# ------------------- CREATE -------------------
@router.post("/sheets")
def create(data: SheetCreate):
    sheet_id = create_sheet_service(data)
    return {"id": sheet_id, "message": "Sheet added successfully"}


# ------------------- GET -------------------
@router.get("/sheets")
def get_all(
    status: bool = Query(None),
    limit: int = 10,
    offset: int = 0
):
    return get_sheets_service(status, limit, offset)


# ------------------- UPDATE -------------------
@router.put("/sheets/{sheet_id}")
def update(sheet_id: int, data: SheetUpdate):
    update_sheet_service(sheet_id, data)
    return {"message": "Sheet updated"}


# ------------------- TOGGLE -------------------
@router.patch("/sheets/{sheet_id}/status")
def toggle(sheet_id: int, data: SheetStatusUpdate):
    update_status_service(sheet_id, data.status)
    return {"message": "Status updated"}


# ------------------- DELETE -------------------
@router.delete("/sheets/{sheet_id}")
def delete(sheet_id: int):
    delete_sheet_service(sheet_id)
    return {"message": "Sheet deleted"}