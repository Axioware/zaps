import logging
from fastapi import APIRouter, Request, HTTPException, Header
from services.salesforce_service import get_sf_access_token
from config.config import SF_INSTANCE_URL
from clients.client import get_client
from utils.retry import safe_request

Router = APIRouter()
logger = logging.getLogger("call_end")

SF_FIELDS = {
    "reason": "Change_of_Mind_Reason__c",
    "interested": "Is_Interested_in_Selling__c",
    "callback": "Check_Back_Time__c"
}

@Router.post("/call-end")
async def handle_call_end(
    request: Request,
    _: str = Header(None)  
):
    try:
        data = await request.json()
        logger.info(f"Webhook received: {data}")

        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")

     
        params = data.get("parameters", {})
        variables = data.get("conversation_initiation_client_data", {}).get("dynamic_variables", {})

        lead_id = (
                variables.get("lead_id") or
                data.get("lead_id")  
            )

        if not lead_id:
            logger.error("Missing lead_id")
            raise HTTPException(status_code=400, detail="Missing lead_id")

        reason = str(params.get("what_changed", "No reason provided"))[:255]
        interested = str(params.get("is_interested", "Unknown"))[:50]
        callback = str(params.get("callback_time", ""))[:50]

        payload = {
            SF_FIELDS["reason"]: reason,
            SF_FIELDS["interested"]: interested,
            SF_FIELDS["callback"]: callback
        }

        access_token = await get_sf_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"

        async with get_client() as client:
            res = await safe_request(
                client,
                "PATCH",
                update_url,
                json=payload,
                headers=headers
            )

        logger.info(f"Lead updated successfully: {lead_id}")
        return {"status": "success"}

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Call-end failure: {str(e)}")
        return {"status": "error", "message": "Internal server error"}