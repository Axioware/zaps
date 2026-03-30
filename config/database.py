import sqlite3, logging, os
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
    with get_connection() as conn:

        # existing table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                google_sheet_url TEXT,
                status BOOLEAN,
                rows_to_process INTEGER,
                cron_schedule TEXT,
                last_run TIMESTAMP,
                last_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ✅ ADD NEW COLUMNS SAFELY
        try:
            conn.execute("ALTER TABLE sheets ADD COLUMN start_time TEXT")
        except:
            pass  # already exists

        try:
            conn.execute("ALTER TABLE sheets ADD COLUMN end_time TEXT")
        except:
            pass  # already exists

        conn.commit()
        logger.info("Database initialized")

def get_row_limit() -> int:
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT num_rows FROM config WHERE id=1"
            ).fetchone()

            if not row:
                raise RuntimeError("Config row missing in DB")

            return row["num_rows"]

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
            logger.info(f"Row limit updated to {new_val}")

    except Exception as e:
        logger.error(f"Error updating row limit: {e}")
        raise