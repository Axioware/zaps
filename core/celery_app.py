import logging, sys, os
from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timedelta, timezone

from config.database import get_connection
from utils.time_utils import is_within_time_window
from api.alab_sheets_bot import trigger_calls
import asyncio


# ------------------- LOGGING -------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ------------------- CELERY INIT -------------------
celery = Celery(
    "scheduler",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

celery.conf.timezone = "UTC"


# ------------------- MAIN SCHEDULER -------------------
@celery.task(bind=True, max_retries=3)
def run_scheduler(self):
    now_utc = datetime.now(timezone.utc)
    now_local_time = now_utc.time()  # time part only
    today = now_utc.strftime("%A")   # Monday, Tuesday, etc.

    try:
        with get_connection() as conn:
            # Select only active sheets
            sheets = conn.execute(
                "SELECT * FROM sheets WHERE status=1"
            ).fetchall()

        for sheet in sheets:
            try:
                sheet_id = sheet["id"]
                start_time = sheet["start_time"]
                end_time = sheet["end_time"]
                day_of_week = sheet["day_of_week"]
                last_run = sheet["last_run"]

                # -------- CHECK DAY OF WEEK --------
                if day_of_week != today:
                    logger.info(f"⏭ Skipping {sheet_id} (not {today})")
                    continue

                # -------- CHECK TIME WINDOW --------
                if start_time and end_time:
                    start_dt = datetime.strptime(start_time, "%H:%M").time()
                    end_dt = datetime.strptime(end_time, "%H:%M").time()
                    if not (start_dt <= now_local_time <= end_dt):
                        logger.info(f"⏭ Skipping {sheet_id} (outside time window)")
                        continue

                # -------- DUPLICATE PREVENT --------
                if last_run:
                    try:
                        last_run_dt = datetime.fromisoformat(last_run)
                        if (now_utc - last_run_dt) < timedelta(minutes=2):
                            logger.info(f"⏭ Skipping {sheet_id} (already ran recently)")
                            continue
                    except Exception:
                        logger.warning(f" Invalid last_run format for sheet {sheet_id}")

                # -------- EXECUTE TASK --------
                process_sheet.delay(sheet_id)

                # -------- UPDATE LAST RUN --------
                with get_connection() as conn:
                    conn.execute(
                        "UPDATE sheets SET last_run=? WHERE id=?",
                        (now_utc.isoformat(), sheet_id)
                    )
                    conn.commit()

            except Exception as e:
                logger.error(f" Sheet {sheet['id']} failed: {e}", exc_info=True)

    except Exception as e:
        logger.error(f" Scheduler failure: {e}", exc_info=True)
        raise self.retry(countdown=10)


# ------------------- WORKER TASK -------------------
@celery.task(bind=True, max_retries=3)
def process_sheet(self, sheet_id):

    try:

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(trigger_calls(sheet_id))
        finally:
            loop.close()

        logger.info(f" Completed sheet {sheet_id}")

    except Exception as e:
        logger.error(f" Sheet {sheet_id} error: {e}")
        raise self.retry(countdown=20)


# ------------------- BEAT SCHEDULE -------------------
celery.conf.beat_schedule = {
    "run-every-2-minutes": {
        "task": "core.celery_app.run_scheduler",
        "schedule": crontab(minute="*/2"),
    },
}