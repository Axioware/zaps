import logging
import os
import re
import json
import requests
from fastapi import Request
import gspread
from fastapi import APIRouter
from google.oauth2.service_account import Credentials
from config.config import (
    ALAB_WORKSHEET_NAME,
    ELEVEN_LABS_KEY,
)
from datetime import datetime
import pytz
AREA_CODE_CACHE = None
# ---------- LOGGING SETUP ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)

Router = APIRouter()

ELEVENLABS_URL = "https://api.elevenlabs.io/v1/convai/twilio/outbound-call"

def get_area_code_map_cached():
    global AREA_CODE_CACHE

    if AREA_CODE_CACHE is None:
        AREA_CODE_CACHE = load_area_code_map()

    return AREA_CODE_CACHE
# ---------- GOOGLE SHEETS CLIENT ----------
def get_client():
    logging.info("Initializing Google Sheets client")

    service_account_info = json.loads(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "{}")
    )

    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )

    client = gspread.authorize(creds)
    logging.info("Google Sheets client initialized successfully")

    return client

def load_area_code_map():
    logging.info("Loading area code mapping from Google Sheet")

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

    logging.info(f"Loaded {len(area_map)} area mappings")

    return area_map

# ---------- STEP 2 ----------
def get_leads(limit=5):
    logging.info(f"Fetching leads with limit={limit}")

    client = get_client()
    sheet = client.open_by_key("1chCOCUqMtZ-q25b2mV1hiwoyx7jLLePFWPkOcCbr7mU").worksheet(ALAB_WORKSHEET_NAME)

    records = sheet.get_all_records()
    logging.info(f"Total records fetched: {len(records)}")

    leads = []
    for idx, r in enumerate(records, start=2):
        if not r.get("Call Disposition"):
            r["_row"] = idx
            leads.append(r)

    logging.info(f"Filtered leads count: {len(leads)}")

    return leads[:limit], sheet


# ---------- STEP 4 ----------
def normalize_phone(valid_phone, mobile_phone):
    logging.info(f"Normalizing phone | valid: {valid_phone}, mobile: {mobile_phone}")

    phone = valid_phone if valid_phone and str(valid_phone).strip() else mobile_phone

    if not phone:
        logging.warning("No phone provided")
        return None, None

    phone = re.sub(r"\D", "", str(phone))

    if len(phone) == 22:
        phone = phone[:11]

    if len(phone) == 10:
        phone = "1" + phone

    if len(phone) < 11:
        logging.warning(f"Invalid phone after normalization: {phone}")
        return None, None

    area = phone[1:4]

    formatted = f"+{phone}"
    logging.info(f"Normalized phone: {formatted}, area: {area}")

    return formatted, area


def get_area_mapping(area):
    logging.info(f"Fetching dynamic area mapping for: {area}")

    area_map = get_area_code_map_cached()

    lis = area_map.get(area, None)

    logging.info(f"Resolved phone_id: {lis}")

    if lis:
        return lis[0], lis[1]
    return None, None


# ---------- STEP 6 ----------
def make_call(phone_id, to_number, address):
    logging.info(f"Making call | to: {to_number}, from_id: {phone_id}, address: {address}")

    payload = {
        "agent_id": "agent_6401kkp1zhqketvbbkqhc9jgnh4c",
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

    logging.info(f"ElevenLabs status code: {res.status_code}")

    try:
        response_json = res.json()
        logging.info(f"ElevenLabs response: {response_json}")
        return response_json
    except Exception:
        logging.error("Failed to parse ElevenLabs response")
        return {}


# ---------- STEP 7 ----------
def remove_plus(phone):
    cleaned = phone.lstrip("+")
    logging.info(f"Removed plus: {phone} -> {cleaned}")
    return cleaned


# ---------- STEP 9 ----------
def find_row_by_phone(sheet, phone):
    logging.info(f"Searching for phone in sheet: {phone}")

    records = sheet.get_all_records()

    for idx, r in enumerate(records, start=2):
        valid = str(r.get("VALID_PHONES", "")).replace("+", "")
        mobile = str(r.get("MOBILE_PHONE", "")).replace("+", "")

        if phone == valid or phone == mobile:
            logging.info(f"Match found at row: {idx}")
            return idx

    logging.warning("No matching row found")
    return None


# ---------- STEP 10 ----------
def update_row(sheet, row_id, call_count, called_from):
    logging.info(f"Updating row {row_id} | count: {call_count}, from: {called_from}")

    sheet.update(f"L{row_id}", [["Not Answered"]])
    sheet.update(f"N{row_id}", [[call_count]])
    sheet.update(f"S{row_id}", [[called_from]])

    logging.info(f"Row {row_id} updated successfully")


# ================= MAIN ENDPOINT =================
def process_trigger_calls():
    try:
        logging.info("GSheet call trigger started (background)")

        leads, sheet = get_leads(limit=5)

        if not leads:
            logging.info("No leads found")
            return

        for lead in leads:
            try:
                logging.info(f"Processing lead: {lead}")

                phone, area = normalize_phone(
                    lead.get("VALID_PHONES"),
                    lead.get("MOBILE_PHONE")
                )

                if not phone:
                    logging.warning("Skipping lead due to invalid phone")
                    continue

                phone_id, called_from = get_area_mapping(area)

                call_res = make_call(
                    phone_id,
                    phone,
                    lead.get("Address")
                )

                clean_phone = remove_plus(phone)

                call_count = int(lead.get("Call_Count") or 0) + 1

                row_id = find_row_by_phone(sheet, clean_phone)

                if not row_id:
                    logging.warning("Skipping update, row not found")
                    continue

                update_row(sheet, row_id, call_count, called_from)

            except Exception as e:
                logging.error(f"Error processing lead: {e}", exc_info=True)

        logging.info("Background call processing complete")

    except Exception as e:
        logging.error(f"Fatal background error: {e}", exc_info=True)
        
@Router.post("/")
async def trigger_calls(background_tasks: BackgroundTasks):
    try:
        logging.info("Trigger endpoint hit")

        background_tasks.add_task(process_trigger_calls)

        return {
            "status": "started",
            "message": "Call processing running in background"
        }

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        return {"error": str(e)}

def process_post_call(payload: dict):
    try:
        client = get_client()
        sheet = client.open_by_key("1chCOCUqMtZ-q25b2mV1hiwoyx7jLLePFWPkOcCbr7mU").worksheet(ALAB_WORKSHEET_NAME)

        called_number = (
            payload.get("conversation_initiation_client_data", {})
            .get("dynamic_variables", {})
            .get("system__called_number")
        )

        if not called_number:
            logging.warning("No called_number found in payload")
            return

        phone = str(called_number).replace("+", "")

        records = sheet.get_all_records()

        row_id = None

        for idx, r in enumerate(records, start=2):
            if str(r.get("VALID_PHONES", "")).replace("+", "") == phone:
                row_id = idx
                break

        if not row_id:
            for idx, r in enumerate(records, start=2):
                if str(r.get("MOBILE_PHONE", "")).replace("+", "") == phone:
                    row_id = idx
                    break

        if not row_id:
            logging.warning("No matching lead found")
            return

        timestamp = payload.get("metadata", {}).get("start_time_unix_secs")
        pacific_time = ""

        if timestamp:
            dt = datetime.utcfromtimestamp(timestamp)
            pacific = pytz.timezone("America/Los_Angeles")
            pacific_time = dt.replace(tzinfo=pytz.utc).astimezone(pacific).strftime("%m/%d/%Y %H:%M:%S")

        analysis_list = payload.get("analysis", {}).get("data_collection_results_list", [])

        def get_value(key):
            for item in analysis_list:
                if item.get("data_collection_id") == key:
                    return item.get("value")
            return None

        metadata = payload.get("metadata", {})

        sheet.update(f"L{row_id}:T{row_id}", [[
            "Answered",
            pacific_time,
            get_value("wrong_call"),
            get_value("Do they want to sell?"),
            get_value("call_back_time"),
            str(metadata.get("features_usage", {}).get("transfer_to_number", {}).get("used")),
            "", 
            metadata.get("call_duration_secs")
        ]])

        logging.info(f"Background update completed for row {row_id}")

    except Exception as e:
        logging.error(f"Background task error: {e}", exc_info=True)
        
        
from fastapi import BackgroundTasks

@Router.post("/post-call")
async def post_call_update(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        payload = data.get("data", {})

        background_tasks.add_task(process_post_call, payload)

        return {"status": "processing"}

    except Exception as e:
        logging.error(f"Post-call error: {e}", exc_info=True)
        return {"error": str(e)}