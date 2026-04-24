from datetime import datetime
import psycopg2
import psycopg2.extras
import logging, os, time
from contextlib import contextmanager
import pytz

logger = logging.getLogger("db")


los_angeles_tz = pytz.timezone("America/Los_Angeles")
los_angeles_time = datetime.now(los_angeles_tz)
timestamp_str = los_angeles_time.strftime("%Y-%m-%d %H:%M:%S PDT")


class _PGConn:
    """Thin wrapper so callers can use conn.execute() like sqlite3."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql, params or ())
        return cur

    def cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


@contextmanager
def get_connection():
    conn = None
    try:
        conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
        yield _PGConn(conn)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def init_db():
    print('Initializing database...')
    with get_connection() as conn:

        # ---------- CONFIG ----------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY,
                num_rows INTEGER NOT NULL CHECK(num_rows > 0)
            )
        """)

        conn.execute("""
            INSERT INTO config (id, num_rows)
            VALUES (1, 5)
            ON CONFLICT (id) DO NOTHING
        """)

        # ---------- SHEETS ----------
        # type:  'google_sheet_job' (default) | 'salesforce_job'
        # query: NULL for sheet jobs, SOQL string for salesforce jobs
        conn.execute("""
        CREATE TABLE IF NOT EXISTS sheets (
            id SERIAL PRIMARY KEY,
            google_sheet_url TEXT,
            worksheet_name TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            status BOOLEAN DEFAULT TRUE,
            last_run TIMESTAMP,
            last_status TEXT,
            type TEXT NOT NULL DEFAULT 'google_sheet_job',
            query TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Drop NOT NULL on google_sheet_url for existing databases
        conn.execute("""
            ALTER TABLE sheets ALTER COLUMN google_sheet_url DROP NOT NULL
        """)

        # Migrate existing tables that don't have the new columns yet
        conn.execute("""
            ALTER TABLE sheets ADD COLUMN IF NOT EXISTS
            type TEXT NOT NULL DEFAULT 'google_sheet_job'
        """)

        conn.execute("""
            ALTER TABLE sheets ADD COLUMN IF NOT EXISTS
            query TEXT
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sheets_status
        ON sheets(status)
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sheets_type
        ON sheets(type)
        """)

        # ---------- SCHEDULE TABLE ----------
        conn.execute("""
        CREATE TABLE IF NOT EXISTS sheet_schedules (
            id SERIAL PRIMARY KEY,
            sheet_id INTEGER NOT NULL,
            day_of_week TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            FOREIGN KEY(sheet_id) REFERENCES sheets(id) ON DELETE CASCADE
        )
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sheet_schedules_sheet_id
        ON sheet_schedules(sheet_id)
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sheet_schedules_day
        ON sheet_schedules(day_of_week)
        """)

        # ---------- CALL LOGS ----------
        conn.execute("""
        CREATE TABLE IF NOT EXISTS call_logs (
            id SERIAL PRIMARY KEY,
            conversation_id TEXT UNIQUE,
            to_number TEXT,
            from_number TEXT,
            lead_id TEXT,
            sheet_id INTEGER REFERENCES sheets(id) ON DELETE SET NULL,
            call_disposition TEXT DEFAULT 'Not Answered',
            called_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            duration_secs INTEGER,
            call_status TEXT,
            wrong_call TEXT,
            wants_to_sell TEXT,
            callback_time TEXT,
            transfer_used TEXT,
            transcript TEXT,
            updated_at TIMESTAMP
        )
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_call_logs_conversation_id
        ON call_logs(conversation_id)
        """)

        conn.commit()
        logger.info("Database initialized")


_row_limit_cache: dict = {"value": None, "expires_at": 0.0}
_ROW_LIMIT_TTL = 60  # seconds


def get_row_limit() -> int:
    if _row_limit_cache["value"] is not None and time.monotonic() < _row_limit_cache["expires_at"]:
        return _row_limit_cache["value"]

    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT num_rows FROM config WHERE id=1"
            ).fetchone()

            if not row:
                raise RuntimeError("Config row missing in DB")

            _row_limit_cache["value"] = row["num_rows"]
            _row_limit_cache["expires_at"] = time.monotonic() + _ROW_LIMIT_TTL
            return _row_limit_cache["value"]

    except Exception as e:
        logger.error(f"Error fetching row limit: {e}")
        raise


def update_row_limit(new_val: int):
    if not isinstance(new_val, int) or new_val <= 0:
        raise ValueError("num_rows must be a positive integer")

    try:
        with get_connection() as conn:
            cursor = conn.execute(
                "UPDATE config SET num_rows=%s WHERE id=1",
                (new_val,)
            )

            if cursor.rowcount == 0:
                raise RuntimeError("Failed to update config")

            conn.commit()
            _row_limit_cache["value"] = None  # invalidate cache
            logger.info(f"Row limit updated to {new_val}")

    except Exception as e:
        logger.error(f"Error updating row limit: {e}")
        raise


def create_call_log(conversation_id: str, to_number: str, from_number: str = None,
                    lead_id: str = None, sheet_id: int = None):
    try:
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO call_logs
                   (conversation_id, to_number, from_number, lead_id, sheet_id, call_disposition)
                   VALUES (%s, %s, %s, %s, %s, 'Not Answered')
                   ON CONFLICT (conversation_id) DO NOTHING""",
                (conversation_id, to_number, from_number, lead_id, sheet_id)
            )
            conn.commit()
            logger.info(f"Call log created: {conversation_id}")
    except Exception as e:
        logger.error(f"Error creating call log: {e}")
        raise


def update_call_log(conversation_id: str, call_disposition: str = None, duration_secs: int = None,
                    call_status: str = None, wrong_call: str = None, wants_to_sell: str = None,
                    callback_time: str = None, transfer_used: str = None, transcript: str = None,
                    timestamp_str: str = None):
    try:
        if timestamp_str is None:
            karachi_tz = pytz.timezone("Asia/Karachi")
            timestamp_str = datetime.now(karachi_tz).strftime("%Y-%m-%d %H:%M:%S PKT")

        with get_connection() as conn:
            conn.execute(
                """UPDATE call_logs SET
                   call_disposition = COALESCE(%s, call_disposition),
                   duration_secs    = COALESCE(%s, duration_secs),
                   call_status      = COALESCE(%s, call_status),
                   wrong_call       = COALESCE(%s, wrong_call),
                   wants_to_sell    = COALESCE(%s, wants_to_sell),
                   callback_time    = COALESCE(%s, callback_time),
                   transfer_used    = COALESCE(%s, transfer_used),
                   transcript       = COALESCE(%s, transcript),
                   updated_at       = %s
                   WHERE conversation_id = %s""",
                (call_disposition, duration_secs, call_status, wrong_call,
                 wants_to_sell, callback_time, transfer_used, transcript,
                 timestamp_str, conversation_id)
            )
            conn.commit()
            logger.info(f"Call log updated: {conversation_id}")
    except Exception as e:
        logger.error(f"Error updating call log: {e}")
        raise


def get_call_log(conversation_id: str):
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM call_logs WHERE conversation_id = %s",
                (conversation_id,)
            ).fetchone()
            if row:
                return dict(row)
            return None
    except Exception as e:
        logger.error(f"Error fetching call log: {e}")
        return None
