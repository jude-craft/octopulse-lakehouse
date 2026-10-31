import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    hour_key       TEXT PRIMARY KEY,   -- e.g. 2025-08-15-3 (hour not zero-padded)
    url            TEXT NOT NULL,
    status         TEXT NOT NULL,      -- DOWNLOADED | MISSING | FAILED
    expected_bytes INTEGER,
    actual_bytes   INTEGER,
    sha256         TEXT,
    attempts       INTEGER NOT NULL DEFAULT 0,
    updated_at     TEXT NOT NULL
)
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(SCHEMA)
    conn.commit()
    return conn


def get_row(conn, hour_key: str):
    return conn.execute("SELECT * FROM files WHERE hour_key = ?", (hour_key,)).fetchone()


def upsert(conn, *, hour_key, url, status, expected_bytes, actual_bytes, sha256, attempts):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """INSERT INTO files (hour_key, url, status, expected_bytes, actual_bytes,
                              sha256, attempts, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(hour_key) DO UPDATE SET
               url=excluded.url, status=excluded.status,
               expected_bytes=excluded.expected_bytes, actual_bytes=excluded.actual_bytes,
               sha256=excluded.sha256, attempts=excluded.attempts,
               updated_at=excluded.updated_at""",
        (hour_key, url, status, expected_bytes, actual_bytes, sha256, attempts, now),
    )
    conn.commit()


def day_summary(conn):
    """Per-date counts by status. A date is complete when downloaded == 24."""
    return conn.execute(
        """SELECT substr(hour_key, 1, 10) AS day,
                  SUM(status = 'DOWNLOADED') AS downloaded,
                  SUM(status = 'MISSING')    AS missing,
                  SUM(status = 'FAILED')     AS failed
           FROM files GROUP BY day ORDER BY day"""
    ).fetchall()
