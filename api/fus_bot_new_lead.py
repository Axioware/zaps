import re
import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from fastapi import APIRouter, BackgroundTasks

from api.alab_sheets_bot import get_area_code_map_cached, get_area_mapping

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
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
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
            logger.info('running query at: ' + datetime.now(ZoneInfo("US/Pacific")).strftime("%Y-%m-%d %H:%M:%S"))
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
            logger.info(f"query finished running at : " + datetime.now(ZoneInfo("US/Pacific")).strftime("%Y-%m-%d %H:%M:%S"))
            logger.info(f"Fetched number of leads: {len(leads)}")
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
            for lead in leads:
                await process_lead(client, lead, headers)
            

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")

# ------------------- LEAD PROCESSING -------------------
async def process_lead(client, lead, headers):
    try:
        
        raw_phone = lead.get("Phone") or ""
        logger.info(f"raw phone: {raw_phone}")
        digits = re.sub(r"\D", "", raw_phone)
        logger.info(f"digits {digits}")

        if not digits:
            logging.info("No digits")
            return

        area_code = digits[1:4]
        from_phone, _ = get_area_mapping(area_code)
        if not from_phone:
            from_phone = DEFAULT_PHONE
            logger.info(f"No mapping for area code {area_code}. Using default phone.")

        # Call API
        await safe_request(
            client,
            "POST",
            "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
            json={
                "agent_id": ELEVEN_AGENT_ID,
                "agent_phone_number_id": from_phone,
                "to_number": digits,
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
        pacific_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000+0000")

        update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead['Id']}"

        await safe_request(
            client,
            "PATCH",
            update_url,
            json={"AI_Bot_Last_Modified_Date_Time__c": pacific_now},
            headers=headers
        )


    except Exception as e:
        logger.error(f"Lead failed {lead.get('Id')}: {str(e)}")