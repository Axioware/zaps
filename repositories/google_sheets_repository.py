import logging, os, gspread, json
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from config.database import get_connection
from datetime import datetime

logger = logging.getLogger("sheets_repo")


def get_client():
    logger.info("Initializing Google Sheets client")

    service_account_info = json.loads(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    )
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    logger.info("Google Sheets client initialized successfully")
    return client


def load_area_code_map():
    logger.info("Loading area code mapping from Google Sheet")
    client = get_client()
    sheet = client.open_by_key("1MvwpbP9gCsq6ODXTvtqNdWvWiot7iW3SfqmXL6P9Pu8").worksheet("BOT area codes (LIVE)")
    records = sheet.get_all_records()
    area_map = {}
    number = None
    for row in records:
        area = str(row.get("Area Code")).strip()
        phone_id = row.get("Phone Number ID")
        number = row.get("Number")

        if area and phone_id:
            area_map[area] = [phone_id, number]
    logger.info(f"Loaded {len(area_map)} area mappings")
    return area_map


def find_row_by_phone(sheet, phone):
    logger.info(f"Searching for phone in sheet: {phone}")
    records = sheet.get_all_records()

    for idx, r in enumerate(records, start=2):
        valid = str(r.get("VALID_PHONES", "")).replace("+", "")
        mobile = str(r.get("MOBILE_PHONE", "")).replace("+", "")

        if phone == valid or phone == mobile:
            logger.info(f"Match found at row: {idx}")
            return idx

    logger.warning("No matching row found")
    return None

_gs_client = None


def get_sheets_client():
    global _gs_client

    if _gs_client:
        return _gs_client

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    service_account_info = json.loads(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    )

    if not service_account_info:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        service_account_info,
        scope
    )

    _gs_client = gspread.authorize(creds)
    return _gs_client


def log_to_sheets(lead_info, lead_id, duration, conv_id):
    try:
        gs_client = get_sheets_client()

        sheet = gs_client.open_by_key("1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok") \
                 .worksheet("Copy of Call Recording Metrics")
        logger.info("Google Sheets client initialized")

        def safe(val):
            return str(val) if val is not None else ""
        headers = sheet.row_values(1)
        
        data_map = {
            "Call ID": safe(conv_id),
            "Lead Name": safe(lead_info.get("Name")),
            "ACQ Manager": safe(lead_info.get("ACQ_Manager__c")),
            "Property Address": safe(
                f"{lead_info.get('Street', '')}, {lead_info.get('City', '')}, {lead_info.get('State', '')} {lead_info.get('PostalCode', '')}"
            ),
            "Call Duration": f"{duration}s",
            "Change of Mind Reason": safe(lead_info.get("Change_of_Mind_Reason__c")),
            "Is Interested?": safe(lead_info.get("is_interested_in_selling__c")),
            "Checkback Time": safe(lead_info.get("check_back_time__c")),
            "Link to Profile": f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view"
        }
    
        row = [data_map.get(col, "") for col in headers]
        logger.info(f"Row before append: {row}")
        sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"✅ Sheet updated for lead {lead_id}")

    except Exception as e:
        logger.error(f"❌ Google Sheets error: {repr(e)}")








# ------------------- CREATE -------------------
def create_sheet(data):
    with get_connection() as conn:
        cursor = conn.execute("""
            INSERT INTO sheets 
            (google_sheet_url, rows_to_process, cron_schedule, status, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            str(data.google_sheet_url),   # ✅ fix HttpUrl
            data.rows_to_process,
            data.cron_schedule,
            int(data.status),             # ✅ store as 0/1
            datetime.utcnow()
        ))
        conn.commit()
        return cursor.lastrowid


# ------------------- READ -------------------
def get_sheets(status=None, limit=10, offset=0):
    with get_connection() as conn:
        query = "SELECT * FROM sheets"
        params = []

        if status is not None:
            query += " WHERE status=?"
            params.append(int(status))   # ✅ ensure 0/1

        query += " ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


# ------------------- UPDATE (FULL) -------------------
def update_sheet(sheet_id, data):
    fields = []
    values = []

    for key, value in data.dict(exclude_none=True).items():

        # ✅ convert any complex type to string
        if not isinstance(value, (int, float, bool, str)):
            value = str(value)

        fields.append(f"{key}=?")
        values.append(value)

    if not fields:
        return False

    values.append(sheet_id)

    with get_connection() as conn:
        conn.execute(
            f"UPDATE sheets SET {', '.join(fields)} WHERE id=?",
            values
        )
        conn.commit()
        return True


# ------------------- UPDATE STATUS -------------------
def update_status(sheet_id, status):
    with get_connection() as conn:
        cursor = conn.execute(
            "UPDATE sheets SET status=? WHERE id=?",
            (int(status), sheet_id)
        )
        conn.commit()

        if cursor.rowcount == 0:
            logger.warning(f"No sheet found to update status id={sheet_id}")
            return False

        return True


# ------------------- DELETE -------------------
def delete_sheet(sheet_id):
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM sheets WHERE id=?",
            (sheet_id,)
        )
        conn.commit()

        if cursor.rowcount == 0:
            logger.warning(f"No sheet found to delete id={sheet_id}")
            return False

        return True


# ------------------- SCHEDULER HELPERS -------------------

def update_last_run(sheet_id):
    with get_connection() as conn:
        conn.execute(
            "UPDATE sheets SET last_run=? WHERE id=?",
            (datetime.utcnow(), sheet_id)
        )
        conn.commit()


def update_last_status(sheet_id, status):
    with get_connection() as conn:
        conn.execute(
            "UPDATE sheets SET last_status=? WHERE id=?",
            (status, sheet_id)
        )
        conn.commit()


# ------------------- OPTIONAL HELPERS -------------------

def get_active_sheets(limit=1000):
    """Used by scheduler"""
    return get_sheets(status=True, limit=limit, offset=0)


def get_sheet_by_id(sheet_id):
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM sheets WHERE id=?",
            (sheet_id,)
        ).fetchone()

        return dict(row) if row else None