from pydantic import BaseModel, HttpUrl
from typing import Optional

class SheetCreate(BaseModel):
    google_sheet_url: str
    rows_to_process: int
    cron_schedule: str
    status: bool
    start_time: str  # ✅ NEW
    end_time: str    # ✅ NEW

class SheetUpdate(BaseModel):
    google_sheet_url: Optional[HttpUrl]
    rows_to_process: Optional[int]
    cron_schedule: Optional[str]
    status: Optional[bool]

class SheetStatusUpdate(BaseModel):
    status: bool

    