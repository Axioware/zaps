"""
api/sf_sheets_bot.py

Salesforce Job Bot  —  mirrors alab_sheets_bot.py but:
  - reads leads from Salesforce using a stored SOQL query (sheets.query)
  - post-call webhook updates the SF Lead record  +  logs to Google Sheets
    (same behaviour as fus_bot_post_call.py)
"""

import asyncio
import logging
import re
from datetime import datetime

import pytz
from fastapi import APIRouter

from clients.client import get_client
from config.config import DEFAULT_PHONE, SF_INSTANCE_URL, ELEVEN_LABS_KEY
from config.database import (
    create_call_log,
    get_connection,
    get_row_limit,
    update_call_log,
    get_call_log,
)
from repositories.google_sheets_repository import log_to_sheets
from services.area_service import get_area_mapping
from services.salesforce_service import get_sf_access_token
from utils.retry import safe_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s",
)

Router = APIRouter()
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  HELPER  —  normalise a raw Salesforce phone string
# ═══════════════════════════════════════════════════════════════
def _clean_phone(raw: str) -> str:
    """Strip non-digits; return empty string if unusable."""
    if not raw or str(raw).strip().lower() in ("", "restricted", "none"):
        return ""
    return re.sub(r"\D", "", raw)


# ═══════════════════════════════════════════════════════════════
#  CELERY ENTRY  —  called by process_sheet() for salesforce_job
# ═══════════════════════════════════════════════════════════════
async def trigger_sf_calls(sheet_id: int):
    """
    1. Load job config from DB (sheets row with type='salesforce_job').
    2. Run the stored SOQL query against Salesforce.
    3. For each lead → resolve area mapping → place ElevenLabs call → log.
    4. Stamp AI_Bot_Last_Modified_Date_Time__c on the SF lead.
    """
    logger.info(f"SF trigger started for sheet_id={sheet_id}")

    try:
        # ── load job row ──────────────────────────────────────────
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM sheets WHERE id=%s AND type='salesforce_job'",
                (sheet_id,),
            ).fetchone()

        if not row:
            logger.error(f"sheet_id={sheet_id} not found or not a salesforce_job")
            return

        row = dict(row)
        soql_query = row.get("query")
        agent_id   = row.get("agent_id")

        if not soql_query:
            logger.error(f"sheet_id={sheet_id} has no SOQL query configured")
            return
        if not agent_id:
            logger.error(f"sheet_id={sheet_id} has no agent_id configured")
            return

        # ── fetch leads from Salesforce ───────────────────────────
        limit        = get_row_limit()
        access_token = await get_sf_access_token()
        sf_headers   = {"Authorization": f"Bearer {access_token}"}
        query_url    = f"{SF_INSTANCE_URL}/services/data/v57.0/query"

        # Inject LIMIT if the user's query doesn't already have one
        soql_with_limit = soql_query
        if "limit" not in soql_query.lower():
            soql_with_limit = soql_query.rstrip().rstrip(";") + f" LIMIT {limit}"

        async with get_client() as client:
            res = await safe_request(
                client, "GET", query_url,
                params={"q": soql_with_limit},
                headers=sf_headers,
            )
            leads = res.json().get("records", [])

        if not leads:
            logger.info(f"No leads returned for sheet_id={sheet_id}")
            return

        results = []

        async with get_client() as client:
            for lead in leads:
                try:
                    await _process_sf_lead(
                        client      = client,
                        lead        = lead,
                        agent_id    = agent_id,
                        sheet_id    = sheet_id,
                        sf_headers  = sf_headers,
                        query_url   = query_url,
                    )
                    results.append({"id": lead.get("Id"), "status": "called"})

                except Exception as e:
                    logger.error(f"Error processing lead {lead.get('Id')}: {e}", exc_info=True)

        logger.info(f"SF job sheet_id={sheet_id} done | processed={len(results)}")
        return {"processed": len(results), "results": results}

    except Exception as e:
        logger.error(f"Fatal error in trigger_sf_calls sheet_id={sheet_id}: {e}", exc_info=True)
        return {"error": str(e)}


async def _process_sf_lead(client, lead, agent_id, sheet_id, sf_headers, query_url):
    """Place one call and update Salesforce timestamp."""
    lead_id   = lead.get("Id")
    raw_phone = lead.get("Phone", "")
    digits    = _clean_phone(raw_phone)

    if not digits:
        logger.warning(f"Lead {lead_id}: no usable phone, skipping")
        return

    # ── area mapping ──────────────────────────────────────────────
    area_code             = digits[1:4] if len(digits) >= 4 else digits[:3]
    phone_id, called_from  = get_area_mapping(area_code)

    if not phone_id:
        logger.info(f"Lead {lead_id}: no area mapping for {area_code}, using default")
        phone_id   = DEFAULT_PHONE
        called_from = DEFAULT_PHONE

    # ── place ElevenLabs call ─────────────────────────────────────
    call_res = await safe_request(
        client,
        "POST",
        "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
        json={
            "agent_id": agent_id,
            "agent_phone_number_id": phone_id,
            "to_number": digits,
            "conversation_initiation_client_data": {
                "dynamic_variables": {
                    "lead_id": lead_id,
                    "address": lead.get("Street") or lead.get("Address") or "See CRM",
                }
            },
        },
        headers={"xi-api-key": ELEVEN_LABS_KEY},
    )

    conv_id = call_res.json().get("conversation_id")

    if conv_id:
        create_call_log(
            conversation_id = conv_id,
            to_number       = digits,
            from_number     = called_from,
            lead_id         = lead_id,
            sheet_id        = sheet_id,
        )

    # ── stamp the SF lead so it won't be picked again immediately ─
    pacific_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    update_url  = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"

    await safe_request(
        client,
        "PATCH",
        update_url,
        json={"AI_Bot_Last_Modified_Date_Time__c": pacific_now},
        headers=sf_headers,
    )
    logger.info(f"SF lead {lead_id} processed, conv_id={conv_id}")


# ═══════════════════════════════════════════════════════════════
#  POST-CALL WEBHOOK  —  /sf-post-call
# ═══════════════════════════════════════════════════════════════
@Router.post("/sf-post-call")
async def sf_post_call(request):
    try:
        data = await request.json()
        logger.info("SF post-call webhook received")

        if not isinstance(data, dict):
            return {"error": "Invalid payload"}

        payload  = data.get("data", {})
        metadata = payload.get("metadata", {})
        duration = int(metadata.get("call_duration_secs", 0))
        call_status = str(payload.get("status", "unknown"))

        # ─────────────────────────────────────────────
        # transcript
        # ─────────────────────────────────────────────
        transcript_lines = []
        for msg in payload.get("transcript", []):
            role = msg.get("role", "").capitalize()
            text = msg.get("message", "")
            if text:
                transcript_lines.append(f"{role}: {text}")

        transcript_str = "\n".join(transcript_lines) or "No transcript"

        # ─────────────────────────────────────────────
        # lead + conversation info
        # ─────────────────────────────────────────────
        custom_data = (
            payload.get("conversation_initiation_client_data", {})
                   .get("dynamic_variables", {})
        )

        lead_id    = custom_data.get("lead_id")
        conv_id    = payload.get("conversation_id")
        call_count = custom_data.get("call_count", 0)

        if not lead_id:
            logger.error("SF post-call: missing lead_id")
            return {"error": "Missing lead_id"}

        # ─────────────────────────────────────────────
        # helper: extract from multiple possible sources
        # ─────────────────────────────────────────────
        def get_field(key):
            return (
                payload.get("analysis", {})
                       .get("data_collection_results", {})
                       .get(key, {})
                       .get("value")
                or payload.get("analysis", {})
                          .get("structured_data", {})
                          .get(key)
                or None
            )

        # ─────────────────────────────────────────────
        # FULL 8 DATA POINTS
        # ─────────────────────────────────────────────
        analysis = {
            "is_looking_to_sell": get_field("is_looking_to_sell"),
            "motivation": get_field("motivation"),
            "fair_cash_price": get_field("fair_cash_price"),
            "roadblocks": get_field("roadblocks"),
            "influencer": get_field("influencer"),
            "timeline": get_field("timeline"),
            "condition": get_field("condition"),
            "next_steps": get_field("next_steps"),
        }

        # ─────────────────────────────────────────────
        # called_from lookup
        # ─────────────────────────────────────────────
        called_from = DEFAULT_PHONE
        if conv_id:
            log = get_call_log(conv_id)
            if log:
                called_from = log.get("from_number") or DEFAULT_PHONE

        # ─────────────────────────────────────────────
        # UPDATE SALESFORCE
        # ─────────────────────────────────────────────
        access_token = await get_sf_access_token()
        sf_headers   = {"Authorization": f"Bearer {access_token}"}
        update_url   = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"

        sf_payload = {
            "Call_Duration__c": float(duration),
            "Call_Status__c": call_status,
            "Call_Transcript__c": transcript_str,
        }

        async with get_client() as client:
            patch_res = await client.patch(update_url, json=sf_payload, headers=sf_headers)
            if patch_res.status_code >= 400:
                raise Exception(f"Salesforce PATCH error: {patch_res.text}")

            get_res = await client.get(update_url, headers=sf_headers)
            lead_info = get_res.json()

        # ─────────────────────────────────────────────
        # GOOGLE SHEETS LOG
        # ─────────────────────────────────────────────
        await asyncio.to_thread(
            log_to_sheets,
            lead_info,
            lead_id,
            duration,
            conv_id,
            call_count=call_count,
            called_from=called_from,
            analysis=analysis,
        )

        # ─────────────────────────────────────────────
        # CALL LOG UPDATE
        # ─────────────────────────────────────────────
        if conv_id:
            update_call_log(
                conversation_id=conv_id,
                call_disposition="Answered" if duration > 0 else "Not Answered",
                duration_secs=duration,
                call_status=call_status,
                transcript=transcript_str,
            )

        logger.info(f"SF post-call completed | lead_id={lead_id}")
        return {"status": "success", "duration": duration}

    except Exception as e:
        logger.error(f"SF post-call error: {e}", exc_info=True)
        return {"status": "error", "message": "Internal error"}