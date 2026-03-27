import logging
import asyncio
from datetime import datetime
from repositories.google_sheets_repository import get_all_sheets

# your existing services
from services.sheets_workflow_service import get_leads, normalize_phone, update_row
from services.area_service import get_area_mapping
from services.call_service import make_call, remove_plus
from services.sheets_helper_service import find_row_by_phone

logger = logging.getLogger("scheduler")


# ------------------- TIME CHECK -------------------
def is_within_time_window(start_time, end_time):
    if not start_time or not end_time:
        return True  # no restriction

    now = datetime.now().time()
    start = datetime.strptime(start_time, "%H:%M").time()
    end = datetime.strptime(end_time, "%H:%M").time()

    return start <= now <= end


# ------------------- MAIN JOB -------------------
async def run_sheet_job(sheet):
    try:
        logger.info(f"Checking sheet {sheet['id']}")

        # ✅ TIME FILTER
        if not is_within_time_window(
            sheet.get("start_time"),
            sheet.get("end_time")
        ):
            logger.info(f"Skipped (outside time): {sheet['id']}")
            return

        logger.info(f"Running job for sheet {sheet['id']}")

        # ✅ GET LEADS (use DB rows_to_process)
        limit = sheet.get("rows_to_process", 5)
        leads, gsheet = get_leads(limit=limit)

        if not leads:
            logger.info("No leads found")
            return

        # ✅ PROCESS EACH LEAD
        for lead in leads:
            try:
                phone, area = normalize_phone(
                    lead.get("VALID_PHONES"),
                    lead.get("MOBILE_PHONE")
                )

                if not phone:
                    continue

                phone_id, called_from = get_area_mapping(area)

                call_res = make_call(
                    phone_id,
                    phone,
                    lead.get("Address")
                )

                clean_phone = remove_plus(phone)

                call_count = int(lead.get("Call_Count") or 0) + 1

                row_id = find_row_by_phone(gsheet, clean_phone)

                if not row_id:
                    continue

                update_row(gsheet, row_id, call_count, called_from)

            except Exception as e:
                logger.error(f"Lead error: {e}")

        logger.info(f"Completed sheet {sheet['id']}")

    except Exception as e:
        logger.error(f"Job failed for sheet {sheet['id']}: {e}")


# ------------------- SCHEDULER LOOP -------------------
async def scheduler_loop():
    while True:
        try:
            sheets = get_all_sheets()

            for sheet in sheets:
                if not sheet["status"]:
                    continue

                # run async job
                asyncio.create_task(run_sheet_job(sheet))

        except Exception as e:
            logger.error(f"Scheduler error: {e}")

        # run every 60 seconds
        await asyncio.sleep(60)