import asyncio, re, logging
from datetime import datetime
from services.salesforce_service import get_sf_access_token
from clients.client import get_client
from utils.retry import safe_request  
from services.area_service import get_area_mapping
from config.config import ELEVEN_AGENT_ID, ELEVEN_LABS_KEY, DEFAULT_PHONE, SF_INSTANCE_URL
from config.database import get_row_limit, create_call_log

logger = logging.getLogger("post_call")
async def run_outbound_workflow():
    try:
        limit = get_row_limit()
        access_token = await get_sf_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        query_url = f"{SF_INSTANCE_URL}/services/data/v57.0/query"

        async with get_client() as client:
            new_query = f"""
            SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
            FROM Lead 
            WHERE AI_Bot_Last_Modified_Date_Time__c = null
            AND Phone != null
            AND Phone != 'Restricted'
            AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
            AND IsConverted = false 
            ORDER BY CreatedDate ASC LIMIT {limit}
            """

            res = await safe_request(client, "GET", query_url, params={"q": new_query}, headers=headers)
            leads = res.json().get("records", [])

            if not leads:
                logger.info("No new leads. Checking old leads...")
                old_query = f"""
                SELECT Id, Phone, Status, CreatedDate, AI_Bot_Last_Modified_Date_Time__c 
                FROM Lead 
                WHERE AI_Bot_Last_Modified_Date_Time__c != null 
                AND Phone != null
                AND Phone != 'Restricted'
                AND (CreatedDate = LAST_N_DAYS:7 OR Status IN ('New Leads', 'Hit List', 'Discovery'))
                AND IsConverted = false 
                ORDER BY AI_Bot_Last_Modified_Date_Time__c ASC LIMIT {limit}
                """

                res = await safe_request(client, "GET", query_url, params={"q": old_query}, headers=headers)
                leads = res.json().get("records", [])

            if not leads:
                logger.info("No leads found.")
                return
            
            for lead in leads:
                await process_lead(client, lead, headers)

    except Exception as e:
        logger.error(f"Workflow failed: {str(e)}")

async def process_lead(client, lead, headers):
    try:
        raw_phone = lead.get("Phone") or ""
        digits = re.sub(r"\D", "", raw_phone)

        if not digits:
            return

        area_code = digits[1:4]
        from_phone, caller_number = get_area_mapping(area_code)
        if not from_phone:
            from_phone = DEFAULT_PHONE
            logger.info(f"No mapping for area code {area_code}. Using default phone.")
        if not caller_number:
            caller_number = from_phone

        call_res = await safe_request(
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

        conv_id = call_res.json().get("conversation_id")
        if conv_id:
            create_call_log(
                conversation_id=conv_id,
                to_number=digits,
                from_number=caller_number,
                lead_id=lead["Id"],
                sheet_id=None
            )

        pacific_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000+0000")
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