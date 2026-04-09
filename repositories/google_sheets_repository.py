import logging, os, gspread, json
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from config.database import get_connection
from datetime import datetime
import pytz

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


from datetime import datetime

def log_to_sheets(lead_info, lead_id, duration, conv_id, analysis=None, call_count=0, called_from=""):
    """
    Logs call info to Google Sheets.

    analysis: dict containing 'wrong_call', 'call_back_time', 'call_transferred' from ElevenLabs agent
    call_count: previous call count, will increment if duration > 0
    called_from: agent number
    """
    try:
        gs_client = get_sheets_client()

        sheet = gs_client.open_by_key("1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok") \
                 .worksheet("Copy of Call Recording Metrics")
        logger.info("Google Sheets client initialized")

        def safe(val):
            return str(val) if val is not None else ""
        if duration > 0:
            call_count = (call_count or 0) + 1
        headers = sheet.row_values(1)
        
        # Get current time in Karachi
        karachi_tz = pytz.timezone("Asia/Karachi")
        karachi_time = datetime.now(karachi_tz)
        timestamp_str = karachi_time.strftime("%Y-%m-%d %H:%M:%S PKT")  # PKT = Pakistan Time

        
        data_map = {
            "Call ID": safe(conv_id),
            "Lead Name": safe(lead_info.get("Name")),
            "ACQ Manager": safe(lead_info.get("ACQ_Manager__c")),
            "Property Address": safe(
                f"{lead_info.get('Street', '')}, {lead_info.get('City', '')}, {lead_info.get('State', '')} {lead_info.get('PostalCode', '')}"
            ),
            "Call Duration": f"{duration}s",
            "Change of Mind Reason": safe(lead_info.get("Change_of_Mind_Reason__c")),
            "Is Interested?": safe(lead_info.get("Is_Interested_in_Selling__c")),
            "Checkback Time": safe(lead_info.get("Check_Back_Time__c")),
            "Link to Profile": f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view",
            "Call Disposition": "Answered" if duration > 0 else "Not Answered",
            "Call Count": str(call_count),
            "Call Back Time": safe(analysis.get("call_back_time") if analysis else ""),
            "Wrong / DNC": safe(analysis.get("wrong_call") if analysis else ""),
            "Was it Transferred": safe(analysis.get("call_transferred") if analysis else ""),
            "Called From": safe(called_from),
            "Timestamp": timestamp_str
        }

        row = [data_map.get(col, "") for col in headers]
        logger.info(f"Row before append: {row}")
        sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"✅ Sheet updated for lead {lead_id} | Call Count: {call_count}")

    except Exception as e:
        logger.error(f"❌ Google Sheets error: {repr(e)}")