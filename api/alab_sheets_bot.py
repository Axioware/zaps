import logging, pytz
from fastapi import APIRouter, Request
from config.config import ALAB_WORKSHEET_NAME
from datetime import datetime
from services.sheets_workflow_service import get_leads, normalize_phone, update_row
from services.area_service import get_area_mapping
from services.call_service import make_call
from repositories.google_sheets_repository import get_client, find_row_by_phone
from utils.phone_utils import remove_plus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)

Router = APIRouter()
ELEVENLABS_URL = "https://api.elevenlabs.io/v1/convai/twilio/outbound-call"


@Router.post("/")
async def trigger_calls():
    try:
        logging.info("GSheet call trigger started")

        leads, sheet = get_leads(limit=5)

        if not leads:
            logging.info("No leads found")
            return {"message": "No leads found"}

        results = []

        for lead in leads:
            try:
                logging.info(f"Processing lead: {lead}")

                phone, area = normalize_phone(
                    lead.get("VALID_PHONES"),
                    lead.get("MOBILE_PHONE")
                )

                if not phone:
                    logging.warning("Skipping lead due to invalid phone")
                    continue

                phone_id, called_from = get_area_mapping(area)

                call_res = make_call(
                    phone_id,
                    phone,
                    lead.get("Address")
                )

                clean_phone = remove_plus(phone)

                call_count = int(lead.get("Call_Count") or 0) + 1
                logging.info(f"New call count: {call_count}")

                row_id = find_row_by_phone(sheet, clean_phone)

                if not row_id:
                    logging.warning("Skipping update, row not found")
                    continue

                update_row(sheet, row_id, call_count, called_from)

                results.append({"phone": phone, "status": "called"})

            except Exception as e:
                logging.error(f"Error processing lead: {e}", exc_info=True)

        logging.info(f"Processing complete. Total processed: {len(results)}")

        return {"processed": len(results), "results": results}

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        return {"error": str(e)}

def process_post_call(payload: dict):
    try:
        client = get_client()
        sheet = client.open_by_key("1chCOCUqMtZ-q25b2mV1hiwoyx7jLLePFWPkOcCbr7mU").worksheet(ALAB_WORKSHEET_NAME)

        called_number = (
            payload.get("conversation_initiation_client_data", {})
            .get("dynamic_variables", {})
            .get("system__called_number")
        )

        if not called_number:
            logging.warning("No called_number found in payload")
            return

        phone = str(called_number).replace("+", "")

        records = sheet.get_all_records()

        row_id = None

        for idx, r in enumerate(records, start=2):
            if str(r.get("VALID_PHONES", "")).replace("+", "") == phone:
                row_id = idx
                break

        if not row_id:
            for idx, r in enumerate(records, start=2):
                if str(r.get("MOBILE_PHONE", "")).replace("+", "") == phone:
                    row_id = idx
                    break

        if not row_id:
            logging.warning("No matching lead found")
            return

        timestamp = payload.get("metadata", {}).get("start_time_unix_secs")
        pacific_time = ""

        if timestamp:
            dt = datetime.utcfromtimestamp(timestamp)
            pacific = pytz.timezone("America/Los_Angeles")
            pacific_time = dt.replace(tzinfo=pytz.utc).astimezone(pacific).strftime("%m/%d/%Y %H:%M:%S")

        analysis_list = payload.get("analysis", {}).get("data_collection_results_list", [])

        def get_value(key):
            for item in analysis_list:
                if item.get("data_collection_id") == key:
                    return item.get("value")
            return None

        metadata = payload.get("metadata", {})

        sheet.update(f"L{row_id}:T{row_id}", [[
            "Answered",
            pacific_time,
            get_value("wrong_call"),
            get_value("Do they want to sell?"),
            get_value("call_back_time"),
            str(metadata.get("features_usage", {}).get("transfer_to_number", {}).get("used")),
            "", 
            metadata.get("call_duration_secs")
        ]])

        logging.info(f"Background update completed for row {row_id}")

    except Exception as e:
        logging.error(f"Background task error: {e}", exc_info=True)
        
        
from fastapi import BackgroundTasks

@Router.post("/post-call")
async def post_call_update(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
        payload = data.get("data", {})

        background_tasks.add_task(process_post_call, payload)

        return {"status": "processing"}

    except Exception as e:
        logging.error(f"Post-call error: {e}", exc_info=True)
        return {"error": str(e)}