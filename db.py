import sqlite3
from datetime import datetime
from typing import Optional


DB_PATH = "queue.db"


ISO = lambda: datetime.utcnow().isoformat() + "Z"


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
id TEXT PRIMARY KEY,
command TEXT NOT NULL,
state TEXT NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
max_retries INTEGER NOT NULL DEFAULT 3,
created_at TEXT NOT NULL,
updated_at TEXT NOT NULL,
next_run TEXT,
last_error TEXT,
output TEXT,
worker_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_nextrun ON jobs(state, next_run);
"""




def get_conn():
conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
conn.row_factory = sqlite3.Row
return conn




def init_db():
conn = get_conn()
conn.executescript(SCHEMA)
conn.close()




def insert_job(job: dict):
now = ISO()
conn = get_conn()
with conn:
conn.execute(
"""
INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run)
VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
""",
(
job["id"], job["command"], job.get("attempts", 0),
job.get("max_retries", 3), job.get("created_at", now),
job.get("updated_at", now), job.get("next_run")
),
)
conn.close()




def list_jobs(state: Optional[str] = None):
conn = get_conn()
cur = conn.cursor()
if state:
cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (state,))
else:
cur.execute("SELECT * FROM jobs ORDER BY created_at")
rows = cur.fetchall()
conn.close()
return rows




def get_job(job_id: str):
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
row = cur.fetchone()
conn.close()
return row
