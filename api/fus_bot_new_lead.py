import re
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, BackgroundTasks

from config.config import (
    SF_CLIENT_ID,
    SF_CLIENT_SECRET,
    SF_REFRESH_TOKEN,
    SF_INSTANCE_URL,
    ELEVEN_LABS_KEY,
    ELEVEN_AGENT_ID,
    AREA_CODE_MAP,
    DEFAULT_PHONE
)
from config.database import get_row_limit

Router = APIRouter()
logger = logging.getLogger("lead_workflow")

# ------------------- ROUTE -------------------
@Router.post("/trigger")
async def trigger_webhook(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_outbound_workflow)
    return {"status": "Workflow started"}

# ------------------- HTTP CLIENT -------------------
def get_client():
    return httpx.AsyncClient(timeout=10.0)

# ------------------- RETRY WRAPPER -------------------
async def safe_request(client, method, url, **kwargs):
    for attempt in range(3):
        try:
            res = await client.request(method, url, **kwargs)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt == 2:
                raise
            await asyncio.sleep(1 * (attempt + 1))

# ------------------- SALESFORCE TOKEN -------------------
async def get_sf_access_token():
    url = "https://login.salesforce.com/services/oauth2/token"
    logger.info("Requesting Salesforce access token...")
    async with get_client() as client:
        res = await safe_request(
            client,
            "POST",
            url,
            data={
                "grant_type": "refresh_token",
                "client_id": SF_CLIENT_ID,
                "client_secret": SF_CLIENT_SECRET,
                "refresh_token": SF_REFRESH_TOKEN
            }
        )
        logger.info("Salesforce token received.")
        data = res.json()
        token = data.get("access_token")
        logger.info(f"Salesforce token: {'***' + token[-4:] if token else 'None'}")
        if not token:
            raise RuntimeError("Salesforce token missing")

        return token

# ------------------- MAIN WORKFLOW -------------------
async def run_outbound_workflow():
    try:
        limit = get_row_limit()
        access_token = await get_sf_access_token()

        headers = {"Authorization": f"Bearer {access_token}"}
        query_url = f"{SF_INSTANCE_URL}/services/data/v57.0/query"

        async with get_client() as client:

            # ------------------- NEW LEADS -------------------
            new_query = f"""
            SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
            FROM Lead 
            WHERE AI_Bot_Last_Modified_Date_Time__c = null 
            AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
            AND IsConverted = false 
            ORDER BY CreatedDate ASC LIMIT {limit}
            """

            res = await safe_request(client, "GET", query_url, params={"q": new_query}, headers=headers)
            leads = res.json().get("records", [])

            # ------------------- FALLBACK -------------------
            if not leads:
                logger.info("No new leads. Checking old leads...")

                old_query = f"""
                SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
                FROM Lead 
                WHERE AI_Bot_Last_Modified_Date_Time__c != null 
                AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
                AND IsConverted = false 
                ORDER BY AI_Bot_Last_Modified_Date_Time__c ASC LIMIT {limit}
                """

                res = await safe_request(client, "GET", query_url, params={"q": old_query}, headers=headers)
                leads = res.json().get("records", [])

            if not leads:
                logger.info("No leads found.")
                return

            # ------------------- PROCESS LEADS (PARALLEL) -------------------
            tasks = [process_lead(client, lead, headers) for lead in leads]
            await asyncio.gather(*tasks)

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")

# ------------------- LEAD PROCESSING -------------------
async def process_lead(client, lead, headers):
    try:
        raw_phone = lead.get("Phone") or ""
        digits = re.sub(r"\D", "", raw_phone)

        if digits.startswith("1") and len(digits) > 10:
            digits = digits[1:]

        if not digits:
            return

        area_code = digits[:3]
        from_phone = AREA_CODE_MAP.get(area_code, DEFAULT_PHONE)

        # Call API
        await safe_request(
            client,
            "POST",
            "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
            json={
                "agent_id": ELEVEN_AGENT_ID,
                "agent_phone_number_id": from_phone,
                "to_number": "+923152526525",
                "conversation_initiation_client_data": {
                    "dynamic_variables": {
                        "lead_id": lead["Id"],
                        "address": "See CRM"
                    }
                }
            },
            headers={"xi-api-key": ELEVEN_LABS_KEY}
        )

        # Update Salesforce
        pacific_now = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S")

        update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead['Id']}"

        await safe_request(
            client,
            "PATCH",
            update_url,
            json={"AI_Bot_Last_Modified_Date_Time__c": pacific_now},
            headers=headers
        )

        logger.info(f"Processed lead {lead['Id']}")

    except Exception as e:
        logger.error(f"Lead failed {lead.get('Id')}: {str(e)}")