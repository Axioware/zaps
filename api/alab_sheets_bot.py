import logging, pytz
from fastapi import APIRouter, Request
from datetime import datetime
from services.sheets_workflow_service import get_leads, normalize_phone, update_row
from services.area_service import get_area_mapping
from services.call_service import make_call
from repositories.google_sheets_repository import get_client, find_row_by_phone
from utils.phone_utils import remove_plus
from utils.sheet_utils import extract_sheet_id
from config.database import get_connection, get_row_limit, create_call_log, update_call_log


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(funcName)s | %(message)s"
)

Router = APIRouter()

logger = logging.getLogger(__name__)
    
# ================= TRIGGER CALLS (CELERY ENTRY) =================
async def trigger_calls(sheet_id: int):
    try:
        logger.info(f" Trigger started for sheet_id={sheet_id}")

        # -------- LOAD SHEET FROM DB --------
        with get_connection() as conn:
            sheet_data = conn.execute(
                "SELECT * FROM sheets WHERE id=?",
                (sheet_id,)
            ).fetchone()

        if not sheet_data:
            logger.error(f"{sheet_id} Sheet not found in DB")
            return

        sheet_data = dict(sheet_data)

        sheet_url = sheet_data["google_sheet_url"]
        worksheet_name = sheet_data["worksheet_name"]

        # -------- CONNECT TO GOOGLE SHEET --------
        client = get_client()
        sheet_key = extract_sheet_id(sheet_url)
        sheet = client.open_by_key(sheet_key).worksheet(worksheet_name)

        # -------- GET LIMIT FROM DB --------
        limit = get_row_limit()

        # -------- GET LEADS --------
        leads = get_leads(sheet, limit=limit)

        if not leads:
            logger.info(f"No leads found for sheet  {sheet_id}")
            return {"message": "No leads found"}

        results = []

        for lead in leads:
            try:

                phone, area = normalize_phone(
                    lead.get("VALID_PHONES"),
                    lead.get("MOBILE_PHONE")
                )

                if not phone:
                    logger.warning(f"Skipping lead due to invalid phone: {lead.get('VALID_PHONES') or lead.get('MOBILE_PHONE')}")
                    continue

                phone_id, called_from = get_area_mapping(area)

                call_resp = make_call(
                    phone_id,
                    phone,
                    lead.get("Address")
                )

                conv_id = call_resp.get("conversation_id")
                if conv_id:
                    create_call_log(
                        conversation_id=conv_id,
                        to_number=phone,
                        from_number=called_from,
                        sheet_id=sheet_id
                    )

                clean_phone = remove_plus(phone)

                call_count = int(lead.get("Call_Count") or 0) + 1

                row_id = find_row_by_phone(sheet, clean_phone)

                if not row_id:
                    logging.warning("Skipping update, row not found")
                    continue

                update_row(sheet, row_id, call_count, called_from)

                results.append({"phone": phone, "status": "called"})

            except Exception as e:
                logging.error(f"Error processing lead: {e}", exc_info=True)

        logging.info(f" Completed sheet {sheet_id} | processed={len(results)}")

        return {"processed": len(results), "results": results}

    except Exception as e:
        logging.error(f" Fatal error: {e}", exc_info=True)
        return {"error": str(e)}

# ================= POST CALL WEBHOOK =================
@Router.post("/post-call")
async def post_call_update(request: Request):
    try:
        logging.info("Post-call webhook received")

        data = await request.json()
        payload = data.get("data", {})

        called_number = (
            payload.get("conversation_initiation_client_data", {})
            .get("dynamic_variables", {})
            .get("system__called_number")
        )

        if not called_number:
            return {"error": "No called_number found"}

        phone = str(called_number).replace("+", "")

        # -------- FIND MATCHING SHEET --------
        client = get_client()

        with get_connection() as conn:
            sheets = conn.execute(
                "SELECT * FROM sheets WHERE status=1"
            ).fetchall()

        row_id = None
        sheet = None

        #  Loop through all sheets to find matching phone
        for s in sheets:
            s = dict(s)

            sheet_key = extract_sheet_id(s["google_sheet_url"])
            worksheet_name = s["worksheet_name"]

            temp_sheet = client.open_by_key(sheet_key).worksheet(worksheet_name)
            records = temp_sheet.get_all_records()

            for idx, r in enumerate(records, start=2):
                if str(r.get("VALID_PHONES", "")).replace("+", "") == phone or \
                   str(r.get("MOBILE_PHONE", "")).replace("+", "") == phone:
                    
                    row_id = idx
                    sheet = temp_sheet
                    logging.info(f"Match found in sheet {s['id']} row {row_id}")
                    break

            if row_id:
                break

        if not row_id or not sheet:
            logging.warning("No matching lead found in any sheet")
            return {"message": "No matching lead"}

        # -------- TIME CONVERSION --------
        timestamp = payload.get("event_timestamp")
        pacific_time = ""

        if timestamp:
            dt = datetime.utcfromtimestamp(timestamp)
            pacific = pytz.timezone("America/Los_Angeles")
            pacific_time = dt.replace(tzinfo=pytz.utc).astimezone(pacific)\
                .strftime("%m/%d/%Y %H:%M:%S")

        # -------- DATA --------
        analysis = payload.get("analysis", {}).get("data_collection_results", {})
        metadata = payload.get("metadata", {})

        # -------- UPDATE SHEET --------
        sheet.update(f"L{row_id}", [["Answered"]])
        sheet.update(f"M{row_id}", [[pacific_time]])
        sheet.update(f"O{row_id}", [[analysis.get("wrong_call", {}).get("value")]])
        sheet.update(f"P{row_id}", [[analysis.get("Do they want to sell?", {}).get("value")]])
        sheet.update(f"Q{row_id}", [[analysis.get("call_back_time", {}).get("value")]])
        sheet.update(f"R{row_id}", [[str(metadata.get("features_usage", {}).get("transfer_to_number", {}).get("used"))]])
        sheet.update(f"T{row_id}", [[metadata.get("call_duration_secs")]])

        logging.info(f" Post-call updated row {row_id}")

        # -------- UPDATE CALL LOG --------
        conv_id = payload.get("conversation_id")
        if conv_id:
            update_call_log(
                conversation_id=conv_id,
                call_disposition="Answered",
                duration_secs=metadata.get("call_duration_secs"),
                call_status=str(payload.get("status", "")),
                wrong_call=str(analysis.get("wrong_call", {}).get("value", "") or ""),
                wants_to_sell=str(analysis.get("Do they want to sell?", {}).get("value", "") or ""),
                callback_time=str(analysis.get("call_back_time", {}).get("value", "") or ""),
                transfer_used=str(metadata.get("features_usage", {}).get("transfer_to_number", {}).get("used", "") or ""),
            )

        return {"status": "updated", "row": row_id}

    except Exception as e:
        logging.error(f" Post-call error: {e}", exc_info=True)
        return {"error": str(e)}