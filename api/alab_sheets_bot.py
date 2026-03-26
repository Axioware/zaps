import logging
import os
import re
import json
import requests
import gspread
from fastapi import APIRouter
from google.oauth2.service_account import Credentials
from config.config import (
    ALAB_WORKSHEET_NAME,
    ELEVEN_LABS_KEY,
    ELEVEN_AGENT_ID,
    AREA_CODE_MAP,
    DEFAULT_PHONE
)

Router = APIRouter()

ELEVENLABS_URL = "https://api.elevenlabs.io/v1/convai/twilio/outbound-call"


# ---------- GOOGLE SHEETS CLIENT ----------
def get_client():
    service_account_info = json.loads(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    )

    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )

    return gspread.authorize(creds)

# ---------- STEP 2 ----------
def get_leads(limit=5):
    client = get_client()
    sheet = client.open_by_key("1chCOCUqMtZ-q25b2mV1hiwoyx7jLLePFWPkOcCbr7mU").worksheet(ALAB_WORKSHEET_NAME)

    records = sheet.get_all_records()
    leads = []
    for idx, r in enumerate(records, start=2):
        if not r.get("Call Disposition"):   # empty cell
            r["_row"] = idx                # store row number
            leads.append(r)

    return leads[:limit], sheet


# ---------- STEP 4 ----------
def normalize_phone(valid_phone, mobile_phone):
    phone = valid_phone if valid_phone and str(valid_phone).strip() else mobile_phone

    if not phone:
        return None, None

    phone = re.sub(r"\D", "", str(phone))

    if len(phone) == 22:
        phone = phone[:11]

    if len(phone) == 10:
        phone = "1" + phone

    if len(phone) < 11:
        return None, None

    area = phone[1:4]

    return f"+{phone}", area


# ---------- STEP 5 (⚡ FAST VERSION - NO SHEETS) ----------
def get_area_mapping(area):
    phone_id = AREA_CODE_MAP.get(area, DEFAULT_PHONE)
    return phone_id, phone_id  # second value used as "called_from"


# ---------- STEP 6 ----------
def make_call(phone_id, to_number, address):
    payload = {
        "agent_id": ELEVEN_AGENT_ID,
        "agent_phone_number_id": phone_id,
        "to_number": to_number,
        "conversation_initiation_client_data": {
            "dynamic_variables": {
                "address": address
            }
        }
    }

    headers = {
        "xi-api-key": ELEVEN_LABS_KEY,
        "Content-Type": "application/json"
    }

    res = requests.post(ELEVENLABS_URL, json=payload, headers=headers)
    return res.json()


# ---------- STEP 7 ----------
def remove_plus(phone):
    return phone.lstrip("+")


# ---------- STEP 9 ----------
def find_row_by_phone(sheet, phone):
    records = sheet.get_all_records()

    for idx, r in enumerate(records, start=2):
        valid = str(r.get("VALID_PHONES", "")).replace("+", "")
        mobile = str(r.get("MOBILE_PHONE", "")).replace("+", "")

        if phone == valid or phone == mobile:
            return idx

    return None

# ---------- STEP 10 ----------
def update_row(sheet, row_id, call_count, called_from):
    sheet.update(f"L{row_id}", [["Not Answered"]])   # Call Disposition
    sheet.update(f"N{row_id}", [[call_count]])       # Call Count
    sheet.update(f"S{row_id}", [[called_from]])      # Called From

# ================= MAIN ENDPOINT =================

@Router.post("/")
async def trigger_calls():
    try:
        logging.info("GSheet call trigger started")

        leads, sheet = get_leads(limit=5)

        if not leads:
            return {"message": "No leads found"}

        results = []

        for lead in leads:
            try:
                phone, area = normalize_phone(
                    lead.get("VALID_PHONES"),
                    lead.get("MOBILE_PHONE")
                )

                if not phone:
                    continue

                phone_id, called_from = get_area_mapping(area)

                call_res = make_call(
                    phone_id,
                    phone,
                    lead.get("Address")
                )

                logging.info(f"Call response: {call_res}")

                clean_phone = remove_plus(phone)

                call_count = int(lead.get("Call_Count") or 0) + 1

                row_id = find_row_by_phone(sheet, clean_phone)

                if not row_id:
                    continue

                update_row(sheet, row_id, call_count, called_from)

                results.append({"phone": phone, "status": "called"})

            except Exception as e:
                logging.error(f"Error processing lead: {e}")

        return {"processed": len(results), "results": results}

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return {"error": str(e)}


# ================= POST CALL =================

@Router.post("/post-call")
async def post_call_update(payload: dict):
    try:
        logging.info("Post-call webhook received")

        client = get_client()
        sheet = client.open_by_key("1chCOCUqMtZ-q25b2mV1hiwoyx7jLLePFWPkOcCbr7mU").worksheet(ALAB_WORKSHEET_NAME)

        called_number = (
            payload.get("data", {})
            .get("conversation_initiation_client_data", {})
            .get("dynamic_variables", {})
            .get("system", {})
            .get("called_number")
        )

        if not called_number:
            return {"error": "No called_number found"}

        phone = str(called_number).replace("+", "")

        records = sheet.get_all_records()

        row_id = None

        # VALID_PHONES
        for idx, r in enumerate(records, start=2):
            if str(r.get("VALID_PHONES", "")).replace("+", "") == phone:
                row_id = idx
                break

        # MOBILE_PHONE fallback
        if not row_id:
            for idx, r in enumerate(records, start=2):
                if str(r.get("MOBILE_PHONE", "")).replace("+", "") == phone:
                    row_id = idx
                    break

        if not row_id:
            return {"message": "No matching lead"}

        from datetime import datetime
        import pytz

        timestamp = payload.get("event_timestamp")
        pacific_time = ""

        if timestamp:
            dt = datetime.utcfromtimestamp(timestamp)
            pacific = pytz.timezone("America/Los_Angeles")
            pacific_time = dt.replace(tzinfo=pytz.utc).astimezone(pacific).strftime("%m/%d/%Y %H:%M:%S")

        analysis = payload.get("data", {}).get("analysis", {}).get("data_collection_results", {})
        metadata = payload.get("data", {}).get("metadata", {})

        sheet.update(f"L{row_id}", [["Answered"]])
        sheet.update(f"Q{row_id}", [[pacific_time]])
        sheet.update(f"R{row_id}", [[analysis.get("wrong_call", {}).get("value")]])
        sheet.update(f"S{row_id}", [[analysis.get("Do they want to sell?", {}).get("value")]])
        sheet.update(f"T{row_id}", [[analysis.get("call_back_time", {}).get("value")]])
        sheet.update(f"U{row_id}", [[str(metadata.get("features_usage", {}).get("transfer_to_number", {}).get("used"))]])
        sheet.update(f"V{row_id}", [[metadata.get("call_duration_secs")]])

        return {"status": "updated", "row": row_id}

    except Exception as e:
        logging.error(f"Post-call error: {e}")
        return {"error": str(e)}