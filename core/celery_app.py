import logging, sys, os
from celery import Celery
from celery.schedules import crontab
from datetime import datetime, timezone, timedelta

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
celery.conf.timezone = "UTC"

# ------------------- MAIN SCHEDULER -------------------
# ------------------- MAIN SCHEDULER -------------------
@celery.task(bind=True, max_retries=3)
def run_scheduler(self):
    now_utc = datetime.now(timezone.utc)
    now_local_time = now_utc.time()  # time part only
    today = now_utc.strftime("%A").lower()  # lowercase to match DB

    try:
        with get_connection() as conn:
            sheets = conn.execute("SELECT * FROM sheets WHERE status=1").fetchall()

            for sheet in sheets:
                sheet_id = sheet["id"]

                # Fetch today's schedules
                schedules = conn.execute("""
                    SELECT * FROM sheet_schedules
                    WHERE sheet_id=? AND lower(day_of_week)=?
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

                    # Correctly handle full-day (start <= now <= end) including midnight wrap
                    if start_dt <= end_dt:
                        in_window = start_dt <= now_local_time <= end_dt
                    else:
                        # overnight schedule (e.g., 22:00 - 02:00)
                        in_window = now_local_time >= start_dt or now_local_time <= end_dt

                    if not in_window:
                        logger.info(f"⏭ Skipping sheet {sheet_id} (outside time window {start_time}-{end_time})")
                        continue

                    # Prevent duplicate run
                    last_run = sheet["last_run"]
                    if last_run:
                        try:
                            last_run_dt = datetime.fromisoformat(last_run)
                            if (now_utc - last_run_dt) < timedelta(minutes=2):
                                logger.info(f"⏭ Skipping sheet {sheet_id} (already ran recently)")
                                continue
                        except Exception:
                            logger.warning(f" Invalid last_run format for sheet {sheet_id}")

                    # Execute
                    process_sheet.delay(sheet_id)

                    # Update last run
                    conn.execute(
                        "UPDATE sheets SET last_run=? WHERE id=?",
                        (now_utc.isoformat(), sheet_id)
                    )
            conn.commit()

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