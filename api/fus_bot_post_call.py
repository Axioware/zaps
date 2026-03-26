import logging
import os
import httpx
import asyncio
import json

import gspread
from fastapi import APIRouter, Request, HTTPException, Header
from oauth2client.service_account import ServiceAccountCredentials

from config.config import SF_INSTANCE_URL, ADMIN_SECRET_KEY
from api.fus_bot_new_lead import get_sf_access_token

Router = APIRouter()
logger = logging.getLogger("post_call")

# ------------------- SECURITY -------------------
def verify_webhook(x_api_key: str = Header(None)):
    if not x_api_key or x_api_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")

# ------------------- HTTP CLIENT -------------------
def get_client():
    return httpx.AsyncClient(timeout=10.0)

# ------------------- RETRY -------------------
async def safe_request(client, method, url, **kwargs):
    for attempt in range(3):
        try:
            res = await client.request(method, url, **kwargs)
            res.raise_for_status()
            return res
        except Exception:
            if attempt == 2:
                raise
            await asyncio.sleep(1 * (attempt + 1))

# ------------------- GOOGLE SHEETS (CACHED) -------------------
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

# ------------------- ROUTE -------------------
@Router.post("/post-call")
async def handle_post_call(request: Request):
    try:
        data = await request.json()
        logger.info("Post-call webhook received")

        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")

        payload = data.get("data", {})

        # ------------------- EXTRACT DATA -------------------
        metadata = payload.get("metadata", {})
        duration = int(metadata.get("call_duration_secs", 0))
        call_status = str(payload.get("status", "unknown"))

        transcript = payload.get("transcript", [])

        if transcript:
            lines = []
            for msg in transcript:
                role = msg.get("role", "").capitalize()
                text = msg.get("message")

                # skip empty or system/tool messages
                if not text:
                    continue

                lines.append(f"{role}: {text}")

            transcript_str = "\n".join(lines)
        else:
            transcript_str = "No transcript"

        custom_data = payload.get("conversation_initiation_client_data", {}) \
                             .get("dynamic_variables", {})
        lead_id = custom_data.get("lead_id")
    
        conv_id = payload.get("conversation_id")

        if not lead_id:
            raise HTTPException(status_code=400, detail="Missing lead_id")

        logger.info(f"Extracted - Lead: {lead_id}, Duration: {duration}, Status: {call_status}")

        # ------------------- SALESFORCE -------------------
        access_token = await get_sf_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        sf_payload = {
            "Call_Duration__c": float(duration),
            "Call_Status__c": call_status,
            "Call_Transcript__c": transcript_str
        }

        async with get_client() as client:
            update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"
            logger.info(f"PATCH URL: {update_url}")
            logger.info(f"Payload: {sf_payload}")

            res = await client.patch(update_url, json=sf_payload, headers=headers)

            logger.info(f"STATUS: {res.status_code}")
            logger.info(f"RESPONSE: {res.text}")

            if res.status_code >= 400:
                raise Exception(f"Salesforce error: {res.text}")

        # ------------------- GOOGLE SHEETS -------------------
        if duration > 180:
            logger.info(f"Logging to Google Sheets (duration: {duration}s)")
            await asyncio.to_thread(log_to_sheets, {"id": lead_id}, lead_id, duration, conv_id)

        return {"status": "success", "duration": duration}

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Post-call error: {str(e)}")
        return {"status": "error", "message": "Internal error"}

# ------------------- SHEETS FUNCTION -------------------
def log_to_sheets(lead_info, lead_id, duration, conv_id):
    try:
        gs_client = get_sheets_client()

        sheet = gs_client.open("Ai Bot FUS Discovery Call List").worksheet("Call Recording Metrics")

        row = [
            conv_id,
            lead_info.get("Name", "N/A"),
            lead_info.get("Who_manages_the_property__c"),
            lead_info.get("Address", "N/A"),
            f"{duration}s",
            lead_info.get("Change_of_Mind_Reason__c"),
            lead_info.get("Is_Interested_in_Selling__c"),
            lead_info.get("Check_Back_Time__c"),
            f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view"
        ]

        sheet.append_row(row)

        logger.info(f"Sheet updated for lead {lead_id}")

    except Exception as e:
        logger.error(f"Google Sheets error: {str(e)}")