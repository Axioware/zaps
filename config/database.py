import sqlite3, logging, os, time
from contextlib import contextmanager


logger = logging.getLogger("db")

DB_PATH = os.getenv("DB_PATH", "settings.db")

@contextmanager
def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        conn.close()

def init_db():
    print('Initializing database...')
    with get_connection() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS config (
                id INTEGER PRIMARY KEY,
                num_rows INTEGER NOT NULL CHECK(num_rows > 0)
            )
        """)

        conn.execute("""
            INSERT OR IGNORE INTO config (id, num_rows)
            VALUES (1, 5)
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS sheets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sheet_url TEXT NOT NULL,
            worksheet_name TEXT NOT NULL,
            status BOOLEAN DEFAULT 1,
            cron_schedule TEXT,
            start_time TEXT,
            end_time TEXT,
            last_run TIMESTAMP,
            last_status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sheets_status
        ON sheets(status)
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS call_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT UNIQUE,
            to_number TEXT,
            from_number TEXT,
            lead_id TEXT,
            sheet_id INTEGER REFERENCES sheets(id),
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
                "UPDATE config SET num_rows=? WHERE id=1",
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
                """INSERT OR IGNORE INTO call_logs
                   (conversation_id, to_number, from_number, lead_id, sheet_id, call_disposition)
                   VALUES (?, ?, ?, ?, ?, 'Not Answered')""",
                (conversation_id, to_number, from_number, lead_id, sheet_id)
            )
            conn.commit()
            logger.info(f"Call log created: {conversation_id}")
    except Exception as e:
        logger.error(f"Error creating call log: {e}")
        raise

def update_call_log(conversation_id: str, call_disposition: str = None, duration_secs: int = None,
                    call_status: str = None, wrong_call: str = None, wants_to_sell: str = None,
                    callback_time: str = None, transfer_used: str = None, transcript: str = None):
    try:
        with get_connection() as conn:
            conn.execute(
                """UPDATE call_logs SET
                   call_disposition = COALESCE(?, call_disposition),
                   duration_secs    = COALESCE(?, duration_secs),
                   call_status      = COALESCE(?, call_status),
                   wrong_call       = COALESCE(?, wrong_call),
                   wants_to_sell    = COALESCE(?, wants_to_sell),
                   callback_time    = COALESCE(?, callback_time),
                   transfer_used    = COALESCE(?, transfer_used),
                   transcript       = COALESCE(?, transcript),
                   updated_at       = CURRENT_TIMESTAMP
                   WHERE conversation_id = ?""",
                (call_disposition, duration_secs, call_status, wrong_call,
                 wants_to_sell, callback_time, transfer_used, transcript, conversation_id)
            )
            conn.commit()
            logger.info(f"Call log updated: {conversation_id}")
    except Exception as e:
        logger.error(f"Error updating call log: {e}")
        raise