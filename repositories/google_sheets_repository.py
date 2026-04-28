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
    sheet = client.open_by_key("1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok").worksheet("BOT area codes (LIVE)")
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



import logging
import re
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)


def log_to_sheets(
    lead_info, lead_id, duration, conv_id,
    analysis=None, call_count=0, called_from="", called_to="",
    sheet_url=None, worksheet_name=None,
):
    """
    Appends a new row to the sheet at trigger time.
    duration=0 → disposition="Not Answered" (placeholder until post-call updates it).
    """
    try:
        print(worksheet_name, sheet_url)
        gs_client = get_sheets_client()

        if sheet_url and worksheet_name:
            sheet_key = sheet_url.split("/spreadsheets/d/")[-1].split("/")[0] if "/spreadsheets/d/" in sheet_url else sheet_url
            sheet = gs_client.open_by_key(sheet_key).worksheet(worksheet_name)
            logger.info(f"Logging to custom sheet: {sheet_key}, worksheet: {worksheet_name}")
        else:
            sheet = gs_client.open_by_key(
                "1bk-G0lD3P9J6MSBYmMYLHfA-_aQ1FO-BTe0x20V6_Ok"
            ).worksheet("Copy of Call Recording Metrics")
            logger.info("Logging to default sheet")

        def safe(val):
            return str(val) if val is not None else ""

        if duration > 0:
            call_count = (call_count or 0) + 1

        headers = sheet.row_values(1)

        transferred = False
        if analysis:
            transferred = str(analysis.get("call_transferred")).lower() == "true"

        if duration == 0:
            disposition = "Not Answered"
        elif transferred:
            disposition = "Transferred"
        else:
            disposition = "Answered"

        los_angeles_tz   = pytz.timezone("America/Los_Angeles")
        los_angeles_time = datetime.now(los_angeles_tz)
        timestamp_str    = los_angeles_time.strftime("%Y-%m-%d %H:%M:%S PDT")

        data_map = {
            "Timestamp":   timestamp_str,
            "Call ID":     safe(conv_id),

            "Call Interrupted":   safe(analysis.get("call_interrupted")   if analysis else ""),
            "Frustrated With AI": safe(analysis.get("frustrated_with_ai") if analysis else ""),

            "Are they looking to sell?": safe(analysis.get("is_looking_to_sell") if analysis else ""),
            "Is Interested?":            safe(analysis.get("is_interested")       if analysis else ""),
            "Motivation":                safe(analysis.get("motivation")          if analysis else ""),
            "Fair Cash Price":           safe(analysis.get("fair_cash_price")     if analysis else ""),
            "Roadblocks":                safe(analysis.get("roadblocks")          if analysis else ""),
            "Influencer":                safe(analysis.get("influencer")          if analysis else ""),
            "timeline":                  safe(analysis.get("timeline")            if analysis else ""),
            "condition":                 safe(analysis.get("condition")           if analysis else ""),
            "Next Steps":                safe(analysis.get("next_steps")          if analysis else ""),
            "Change of Mind Reason":     safe(analysis.get("change_of_mind_reason") if analysis else ""),
            "Checkback Time":            safe(analysis.get("checkback_time")      if analysis else ""),

            "Called From":      safe(called_from),
            "Called To":        safe(called_to),
            "Call Duration":    f"{duration}s",
            "Call Disposition": disposition,
            "Call Count":       str(call_count),

            "Lead Name":      safe(lead_info.get("Name")),
            "ACQ Manager":    safe(lead_info.get("ACQ_Manager__c")),
            "Property Address": safe(
                f"{lead_info.get('Street', '')}, {lead_info.get('City', '')}, "
                f"{lead_info.get('State', '')} {lead_info.get('PostalCode', '')}"
            ),
            "Link to Profile": f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view",
        }

        row = [data_map.get(col, "") for col in headers]
        logger.info(f"Row before append: {row}")
        sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"Sheet row added for lead {lead_id} | disposition={disposition}")

    except Exception as e:
        logger.error(f"Google Sheets error (log_to_sheets): {repr(e)}")


def update_sheet_row(
    lead_info, lead_id, duration, conv_id,
    analysis=None, call_count=0, called_from="", called_to="",
    sheet_url=None, worksheet_name=None,
):
    """
    Finds the existing row for this call (matched by 'Called To' phone number)
    and overwrites every column with the final post-call data.

    This is called by the post-call webhook instead of log_to_sheets so we never
    duplicate rows — we only update the placeholder row added at trigger time.
    """
    try:
        if not sheet_url or not worksheet_name:
            logger.warning("update_sheet_row: sheet_url or worksheet_name missing, skipping")
            return

        gs_client = get_sheets_client()
        sheet_key = sheet_url.split("/spreadsheets/d/")[-1].split("/")[0] if "/spreadsheets/d/" in sheet_url else sheet_url
        sheet     = gs_client.open_by_key(sheet_key).worksheet(worksheet_name)
        logger.info(f"update_sheet_row: opened {sheet_key} / {worksheet_name}")

        def safe(val):
            return str(val) if val is not None else ""

        if duration > 0:
            call_count = (call_count or 0) + 1

        headers = sheet.row_values(1)

        transferred = False
        if analysis:
            transferred = str(analysis.get("call_transferred", "")).lower() == "true"

        if duration == 0:
            disposition = "Not Answered"
        elif transferred:
            disposition = "Transferred"
        else:
            disposition = "Answered"

        los_angeles_tz   = pytz.timezone("America/Los_Angeles")
        los_angeles_time = datetime.now(los_angeles_tz)
        timestamp_str    = los_angeles_time.strftime("%Y-%m-%d %H:%M:%S PDT")

        # Full data map — same keys as log_to_sheets so columns always align
        data_map = {
            "Timestamp":   timestamp_str,
            "Call ID":     safe(conv_id),

            "Call Interrupted":   safe(analysis.get("call_interrupted")   if analysis else ""),
            "Frustrated With AI": safe(analysis.get("frustrated_with_ai") if analysis else ""),

            "Are they looking to sell?": safe(analysis.get("is_looking_to_sell") if analysis else ""),
            "Is Interested?":            safe(analysis.get("is_interested")       if analysis else ""),
            "Motivation":                safe(analysis.get("motivation")          if analysis else ""),
            "Fair Cash Price":           safe(analysis.get("fair_cash_price")     if analysis else ""),
            "Roadblocks":                safe(analysis.get("roadblocks")          if analysis else ""),
            "Influencer":                safe(analysis.get("influencer")          if analysis else ""),
            "timeline":                  safe(analysis.get("timeline")            if analysis else ""),
            "condition":                 safe(analysis.get("condition")           if analysis else ""),
            "Next Steps":                safe(analysis.get("next_steps")          if analysis else ""),
            "Change of Mind Reason":     safe(analysis.get("change_of_mind_reason") if analysis else ""),
            "Checkback Time":            safe(analysis.get("checkback_time")      if analysis else ""),

            "Called From":      safe(called_from),
            "Called To":        safe(called_to),
            "Call Duration":    f"{duration}s",
            "Call Disposition": disposition,
            "Call Count":       str(call_count),

            "Lead Name":      safe(lead_info.get("Name")),
            "ACQ Manager":    safe(lead_info.get("ACQ_Manager__c")),
            "Property Address": safe(
                f"{lead_info.get('Street', '')}, {lead_info.get('City', '')}, "
                f"{lead_info.get('State', '')} {lead_info.get('PostalCode', '')}"
            ),
            "Link to Profile": f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view",
        }

        #  Find the row whose "Called To" matches this call 
        # Normalise both sides to digits-only for a reliable match.
        called_to_digits = re.sub(r"\D", "", called_to)

        # Determine which column index holds "Called To"
        try:
            called_to_col = headers.index("Called To") + 1   # 1-based
        except ValueError:
            logger.error("update_sheet_row: 'Called To' column not found in sheet headers")
            return

        all_values  = sheet.get_all_values()  # list of rows (each row = list of cell strings)
        matched_row = None

        for row_idx, row_values in enumerate(all_values[1:], start=2):  # skip header row
            cell_val = row_values[called_to_col - 1] if len(row_values) >= called_to_col else ""
            if re.sub(r"\D", "", cell_val) == called_to_digits:
                matched_row = row_idx
                # Don't break — use the LAST match so we always update the most
                # recent call for this number (handles repeat calls gracefully).

        if matched_row is None:
            logger.warning(
                f"update_sheet_row: no row found for called_to={called_to} "
                f"(lead {lead_id}) — appending new row as fallback"
            )
            row = [data_map.get(col, "") for col in headers]
            sheet.append_row(row, value_input_option="USER_ENTERED")
            return

        #  Overwrite the matched row in a single batch_update call ─
        new_row = [data_map.get(col, "") for col in headers]
        # batch_update expects: range in A1 notation + 2-D values array
        col_end = len(headers)
        range_a1 = f"A{matched_row}:{chr(64 + col_end)}{matched_row}" if col_end <= 26 else f"A{matched_row}:AZ{matched_row}"

        sheet.update(range_a1, [new_row], value_input_option="USER_ENTERED")
        logger.info(
            f"update_sheet_row: row {matched_row} updated for lead {lead_id} "
            f"| disposition={disposition} | duration={duration}s"
        )

    except Exception as e:
        logger.error(f"Google Sheets error (update_sheet_row): {repr(e)}")