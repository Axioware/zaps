import logging
import re
from config.database import get_row_limit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sheets_workflow")


def get_leads(sheet, limit=None):
    try:
        # -------- DYNAMIC LIMIT --------
        if limit is None:
            limit = get_row_limit()

        # safety (avoid 0 or negative)
        limit = max(1, int(limit))

        if limit == 1:
            logger.info('Using dynamic limit from DB: 1 lead')


        # -------- FETCH RECORDS --------
        records = sheet.get_all_records()

        leads = []

        # -------- FILTER LEADS --------
        for idx, r in enumerate(records, start=2):
            if not r.get("Call Disposition"):
                r["_row"] = idx
                leads.append(r)


        # -------- APPLY LIMIT --------
        final_leads = leads[:limit]

        return final_leads

    except Exception as e:
        logger.error(f"Error fetching leads: {e}", exc_info=True)
        return []

def normalize_phone(valid_phone, mobile_phone):
    logger.info(f"Normalizing phone | valid: {valid_phone}, mobile: {mobile_phone}")
    phone = valid_phone if valid_phone and str(valid_phone).strip() else mobile_phone
    if not phone:
        logger.warning("No phone provided")
        return None, None
    phone = re.sub(r"\D", "", str(phone))

    if len(phone) == 22:
        phone = phone[:11]

    if len(phone) == 10:
        phone = "1" + phone

    if len(phone) < 11:
        logger.warning(f"Invalid phone after normalization: {phone}")
        return None, None

    area = phone[1:4]

    formatted = f"+{phone}"
    logger.info(f"Normalized phone: {formatted}, area: {area}")

    return formatted, area


def update_row(sheet, row_id, call_count, called_from):
    logger.info(f"Updating row {row_id} | count: {call_count}, from: {called_from}")
    sheet.update(f"L{row_id}", [["Not Answered"]])
    sheet.update(f"N{row_id}", [[call_count]])
    sheet.update(f"S{row_id}", [[called_from]])
    logger.info(f"Row {row_id} updated successfully")