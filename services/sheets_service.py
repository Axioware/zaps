from repositories.google_sheets_repository import (
    create_sheet,
    get_sheets,
    update_sheet,
    update_status,
    delete_sheet
)

def create_sheet_service(data):
    return create_sheet(data)

def get_sheets_service(status, limit, offset):
    return get_sheets(status, limit, offset)

def update_sheet_service(sheet_id, data):
    return update_sheet(sheet_id, data)

def update_status_service(sheet_id, status):
    update_status(sheet_id, status)

def delete_sheet_service(sheet_id):
    delete_sheet(sheet_id)