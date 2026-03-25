import logging
import httpx
from fastapi import APIRouter, Request
from config.config import SF_INSTANCE_URL
from api.fus_bot_new_lead import get_sf_access_token

Router = APIRouter()

# These remain the same as your Salesforce hasn't changed
SF_FIELDS = {
    "reason": "Change_of_Mind_Reason__c",
    "interested": "Is_Interested_in_Selling__c",
    "callback": "Check_Back_Time__c"
}

@Router.post("/call-end")
async def handle_call_end(request: Request):
    try:
        # 1. Catch the Tool Webhook Data
        data = await request.json()
        logging.info(f"Received Tool Webhook: {data}")

        # 2. Extract Tool Parameters (This is the fix for your Tool setup)
        # Your tool passes: is_interested, what_changed, callback_time
        params = data.get("parameters", {})
        
        # 3. Extract lead_id (Still comes from dynamic_variables)
        variables = data.get("conversation_initiation_client_data", {}).get("dynamic_variables", {})
        lead_id = variables.get("lead_id")

        if not lead_id:
            logging.error("No lead_id found. Check if ElevenLabs is passing dynamic_variables.")
            return {"status": "error", "message": "Missing lead_id"}

        # 4. Prepare Salesforce Update
        access_token = await get_sf_access_token()
        sf_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        # Mapping the TOOL parameters to your SF API names
        payload = {
            SF_FIELDS["reason"]: params.get("what_changed", "No reason provided"),
            SF_FIELDS["interested"]: str(params.get("is_interested", "Unknown")),
            SF_FIELDS["callback"]: params.get("callback_time", "")
        }

        # 5. Execute Update
        update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"
        
        async with httpx.AsyncClient() as client:
            res = await client.patch(update_url, json=payload, headers=sf_headers)
            
            if res.status_code == 204:
                logging.info(f"SUCCESS: Lead {lead_id} updated via Tool.")
                return {"status": "success"}
            else:
                logging.error(f"SF Error ({res.status_code}): {res.text}")
                return {"status": "error", "sf_response": res.text}

    except Exception as e:
        logging.critical(f"Call-End Tool System Failure: {str(e)}")
        return {"status": "error", "message": str(e)}