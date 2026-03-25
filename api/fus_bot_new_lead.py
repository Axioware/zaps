import re
from fastapi import APIRouter, BackgroundTasks
import httpx
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from config.config import *
from config.database import get_row_limit

Router = APIRouter()

@Router.post("/trigger")
async def trigger_webhook(background_tasks: BackgroundTasks):
    # This now triggers the combined "New then Old" logic
    background_tasks.add_task(run_outbound_workflow)
    return {"status": "Workflow initiated (New Leads prioritized)"}

async def get_sf_access_token():
    url = "https://login.salesforce.com/services/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
        "refresh_token": SF_REFRESH_TOKEN
    }
    async with httpx.AsyncClient() as client:
        res = await client.post(url, data=data)
        res.raise_for_status()
        return res.json().get("access_token")

async def run_outbound_workflow():
    try:
        limit = get_row_limit()
        access_token = await get_sf_access_token()
        
        async with httpx.AsyncClient() as client:
            sf_headers = {"Authorization": f"Bearer {access_token}"}
            query_url = f"{SF_INSTANCE_URL}/services/data/v57.0/query"

            # --- STEP 1: TRY NEW LEADS ---
            new_leads_soql = f"""
            SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
            FROM Lead 
            WHERE AI_Bot_Last_Modified_Date_Time__c = null 
            AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
            AND IsConverted = false 
            ORDER BY CreatedDate ASC LIMIT {limit}
            """
            
            res = await client.get(query_url, params={"q": new_leads_soql}, headers=sf_headers)
            leads = res.json().get("records", [])

            # --- STEP 2: FAILOVER TO OLD LEADS IF EMPTY ---
            if not leads:
                logging.info("No NEW leads found. Checking for OLD leads...")
                old_leads_soql = f"""
                SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
                FROM Lead 
                WHERE AI_Bot_Last_Modified_Date_Time__c != null 
                AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
                AND IsConverted = false 
                ORDER BY AI_Bot_Last_Modified_Date_Time__c ASC LIMIT {limit}
                """
                res = await client.get(query_url, params={"q": old_leads_soql}, headers=sf_headers)
                leads = res.json().get("records", [])

            if not leads:
                logging.info("No leads (New or Old) found to process.")
                return

            # --- STEP 3: PROCESS THE LEADS ---
            for lead in leads:
                # Phone formatting
                raw_phone = lead.get('Phone', '') or ''
                digits = re.sub(r'\D', '', raw_phone)
                if digits.startswith('1') and len(digits) > 10:
                    digits = digits[1:]
                
                if not digits:
                    continue

                area_code = digits[:3]
                from_phone = AREA_CODE_MAP.get(area_code, DEFAULT_PHONE)

                # ElevenLabs Outbound Call
                try:
                    await client.post(
                        "https://api.elevenlabs.io/v1/convai/twilio/outbound-call",
                        json={
                            "agent_id": ELEVEN_AGENT_ID,
                            "agent_phone_number_id": from_phone,
                            "to_number": digits,
                            "conversation_initiation_client_data": {
                                "dynamic_variables": {"lead_id": lead['Id'], "address": "See CRM"}
                            }
                        },
                        headers={"xi-api-key": ELEVEN_LABS_KEY}
                    )
                    
                    # Update Salesforce Timestamp
                    pacific_now = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d %H:%M:%S")
                    update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead['Id']}"
                    await client.patch(update_url, json={"AI_Bot_Last_Modified_Date_Time__c": pacific_now}, headers=sf_headers)
                    
                    logging.info(f"Successfully processed lead: {lead['Id']}")
                except Exception as call_err:
                    logging.error(f"Failed to call lead {lead['Id']}: {str(call_err)}")

    except Exception as e:
        logging.error(f"Workflow Critical Error: {str(e)}")