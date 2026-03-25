import logging
import httpx
import gspread
from fastapi import APIRouter, Request
from oauth2client.service_account import ServiceAccountCredentials
from config.config import SF_INSTANCE_URL
from api.fus_bot_new_lead import get_sf_access_token

Router = APIRouter()

# --- GOOGLE SHEETS SETUP ---
def get_sheets_client():
    """Authenticates using the service_account.json file."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # Ensure the file is named exactly service_account.json in your project root
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    return gspread.authorize(creds)

@Router.post("/post-call")
async def handle_post_call(request: Request):
    try:
        data = await request.json()
        
        # 1. Extract Metadata from ElevenLabs Webhook
        metadata = data.get("metadata", {})
        duration = int(metadata.get("call_duration_secs", 0))
        call_status = data.get("status", "unknown")
        transcript = data.get("transcript", "No transcript available")
        
        # Extract variables passed during call initiation
        custom_data = data.get("conversation_initiation_client_data", {}).get("dynamic_variables", {})
        lead_id = custom_data.get("lead_id")
        conv_id = data.get("conversation_id")

        if not lead_id:
            logging.error("Post-Call received but no lead_id found.")
            return {"status": "error", "message": "No lead_id"}

        # 2. Salesforce Update & Data Enrichment
        access_token = await get_sf_access_token()
        sf_headers = {"Authorization": f"Bearer {access_token}"}
        
        # First, update the lead with the final transcript and duration
        sf_payload = {
            "Call_Duration__c": f"{duration} seconds",
            "Call_Status__c": call_status,
            "Call_Transcript__c": transcript
        }

        async with httpx.AsyncClient() as client:
            # PATCH: Update transcript in Salesforce
            update_url = f"{SF_INSTANCE_URL}/services/data/v57.0/sobjects/Lead/{lead_id}"
            await client.patch(update_url, json=sf_payload, headers=sf_headers)
            
            # GET: Fetch latest Lead info (to get Name, ACQ Manager, and Tool Results)
            lead_res = await client.get(update_url, headers=sf_headers)
            lead_info = lead_res.json()

        # 3. Discovery Call Logic (Only logs to Sheets if call > 3 minutes)
        if duration > 180:
            logging.info(f"Call {conv_id} lasted {duration}s. Logging to Google Sheets.")
            
            # Open the spreadsheet and specific worksheet
            gs_client = get_sheets_client()
            sheet = gs_client.open("Ai Bot FUS Discovery Call List").worksheet("Call Recording Metrics")
            
            # Prepare the row exactly as your old Zapier workflow did
            row = [
                conv_id,                                      # Column A: Call ID
                lead_info.get("Name", "N/A"),                 # Column B: Lead Name
                lead_info.get("Who_manages_the_property__c"), # Column C: ACQ Manager
                lead_info.get("Address", "N/A"),              # Column D: Property Address
                f"{duration}s",                               # Column E: Call Duration
                lead_info.get("Change_of_Mind_Reason__c"),    # Column F: Reason (from Tool)
                lead_info.get("Is_Interested_in_Selling__c"), # Column G: Interested (from Tool)
                lead_info.get("Check_Back_Time__c"),          # Column H: Callback Time
                f"https://leftmain-4606.lightning.force.com/lightning/r/Lead/{lead_id}/view" # Column I: Link
            ]
            
            sheet.append_row(row)
            logging.info(f"Successfully added row to Google Sheet for lead {lead_id}")

        return {"status": "success", "duration": duration}

    except Exception as e:
        logging.error(f"Post-Call Critical Error: {str(e)}")
        return {"status": "error", "detail": str(e)}