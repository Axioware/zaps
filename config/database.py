import sqlite3

DB_PATH = "settings.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY, num_rows INTEGER)")
        conn.execute("INSERT OR IGNORE INTO config (id, num_rows) VALUES (1, 5)")
        conn.commit()

def get_row_limit():
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT num_rows FROM config WHERE id=1").fetchone()[0]

def update_row_limit(new_val: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE config SET num_rows=? WHERE id=1", (new_val,))
        conn.commit()