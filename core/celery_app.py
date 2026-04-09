import logging, sys, os
from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timedelta
import pytz  

from config.database import get_connection
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

# Set to your local timezone so crontab and logs align with your clock
celery.conf.timezone = "Asia/Karachi"
LOCAL_TZ = pytz.timezone("Asia/Karachi")

# ------------------- MAIN SCHEDULER -------------------
@celery.task(bind=True, max_retries=3)
def run_scheduler(self):
    # Get current time in your local timezone
    now_local = datetime.now(LOCAL_TZ)
    now_time_only = now_local.time()
    today = now_local.strftime("%A").lower()

    try:
        with get_connection() as conn:
            sheets = conn.execute("SELECT * FROM sheets WHERE status=TRUE").fetchall()

            for sheet in sheets:
                sheet_id = sheet["id"]
                logger.info(f"Checking sheet {sheet_id} ({sheet['google_sheet_url']})")
                # Fetch today's schedules
                schedules = conn.execute("""
                    SELECT * FROM sheet_schedules
                    WHERE sheet_id=%s AND lower(day_of_week)=%s
                """, (sheet_id, today)).fetchall()

                if not schedules:
                    logger.info(f"⏭ Skipping sheet {sheet_id} (no schedule for {today})")
                    continue

                for sched in schedules:
                    start_time = sched["start_time"]
                    end_time = sched["end_time"]

                    # Treat "00:00-00:00" as inactive
                    if start_time == "00:00" and end_time == "00:00":
                        logger.info(f"⏭ Skipping sheet {sheet_id} ({today} marked inactive)")
                        continue

                    start_dt = datetime.strptime(start_time, "%H:%M").time()
                    end_dt = datetime.strptime(end_time, "%H:%M").time()

                    # Handle window check (including overnight support)
                    if start_dt <= end_dt:
                        in_window = start_dt <= now_time_only <= end_dt
                    else:
                        # overnight schedule (e.g., 22:00 - 02:00)
                        in_window = now_time_only >= start_dt or now_time_only <= end_dt

                    if not in_window:
                        logger.info(f"⏭ Skipping sheet {sheet_id} (outside window {start_time}-{end_time} vs current {now_time_only.strftime('%H:%M')})")
                        continue

                    # Prevent duplicate run (Check against local timestamp)
                    last_run = sheet["last_run"]
                    if last_run:
                        try:
                            last_run_dt = datetime.fromisoformat(last_run)
                            # Ensure last_run_dt is timezone aware for comparison
                            if last_run_dt.tzinfo is None:
                                last_run_dt = LOCAL_TZ.localize(last_run_dt)
                                
                            if (now_local - last_run_dt) < timedelta(minutes=2):
                                logger.info(f"⏭ Skipping sheet {sheet_id} (already ran at {last_run_dt.strftime('%H:%M')})")
                                continue
                        except Exception as e:
                            logger.warning(f"Invalid last_run format for sheet {sheet_id}: {e}")

                    # Execute
                    process_sheet.delay(sheet_id)

                    # Update last run using local ISO format
                    conn.execute(
                        "UPDATE sheets SET last_run=%s WHERE id=%s",
                        (now_local.isoformat(), sheet_id)
                    )
            conn.commit()

    except Exception as e:
        logger.error(f"Scheduler failure: {e}", exc_info=True)
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
        logger.info(f"Completed sheet {sheet_id}")
    except Exception as e:
        logger.error(f"Sheet {sheet_id} error: {e}")
        raise self.retry(countdown=20)


# ------------------- BEAT SCHEDULE -------------------
celery.conf.beat_schedule = {
    "run-every-2-minutes": {
        "task": "core.celery_app.run_scheduler",
        "schedule": crontab(minute="*/2"),
    },
}