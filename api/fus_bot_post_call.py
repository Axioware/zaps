import logging, asyncio
from fastapi import APIRouter, Request, HTTPException, Header
from config.config import SF_INSTANCE_URL
from clients.client import get_client
from repositories.google_sheets_repository import log_to_sheets, get_client
from services.salesforce_service import get_sf_access_token
from config.database import update_call_log

Router = APIRouter()
logger = logging.getLogger("post_call")

@Router.post("/post-call")
async def handle_post_call(request: Request):
    try:
        data = await request.json()
        logger.info("Post-call webhook received")

        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid payload")

        payload = data.get("data", {})

        metadata = payload.get("metadata", {})
        duration = int(metadata.get("call_duration_secs", 0))
        call_status = str(payload.get("status", "unknown"))

        transcript = payload.get("transcript", [])

        if transcript:
            lines = []
            for msg in transcript:
                role = msg.get("role", "").capitalize()
                text = msg.get("message")

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

            res_get = await client.get(update_url, headers=headers)
            lead_info = res_get.json()

            logger.info(f"Fetched Lead Data: {lead_info}")

        # if duration > 18:
        logger.info(f"Logging to Google Sheets (duration: {duration}s)")
        await asyncio.to_thread(log_to_sheets, lead_info, lead_id, duration, conv_id)

        if conv_id:
            update_call_log(
                conversation_id=conv_id,
                call_disposition="Answered" if duration > 0 else "Not Answered",
                duration_secs=duration,
                call_status=call_status,
                transcript=transcript_str,
            )

        return {"status": "success", "duration": duration}

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"Post-call error: {str(e)}")
        return {"status": "error", "message": "Internal error"}
