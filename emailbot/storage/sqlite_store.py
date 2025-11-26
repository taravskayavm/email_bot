"""SQLite-backed storage helpers."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

DB_PATH = Path(os.getenv("EMAILBOT_SQLITE_PATH", "var/emailbot.db")).resolve()

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS blocked_emails (
  email TEXT PRIMARY KEY,
  ts    TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS send_stats (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  ts       TEXT DEFAULT (datetime('now')),
  email    TEXT,
  status   TEXT,
  override INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_send_stats_email ON send_stats(email);
"""


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init() -> None:
    with _conn() as conn:
        conn.executescript(SCHEMA)


def add_blocked(email: str) -> None:
    if not email:
        return
    cleaned = email.strip().lower()
    if not cleaned:
        return
    with _conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_emails(email) VALUES (?)",
            (cleaned,),
        )


def is_blocked(email: str) -> bool:
    if not email:
        return False
    cleaned = email.strip().lower()
    if not cleaned:
        return False
    with _conn() as conn:
        cur = conn.execute(
            "SELECT 1 FROM blocked_emails WHERE email = ? LIMIT 1",
            (cleaned,),
        )
        row = cur.fetchone()
    return row is not None


def list_blocked(limit: int = 100, offset: int = 0) -> Iterable[Tuple[str, str]]:
    with _conn() as conn:
        cur = conn.execute(
            """
            SELECT email, ts
            FROM blocked_emails
            ORDER BY ts DESC
            LIMIT ? OFFSET ?
            """,
            (int(limit), int(offset)),
        )
        yield from cur.fetchall()


def audit_add(email: str, status: str, override: bool = False) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO send_stats(email, status, override) VALUES (?,?,?)",
            (email, status, int(bool(override))),
        )


__all__ = [
    "DB_PATH",
    "init",
    "add_blocked",
    "is_blocked",
    "list_blocked",
    "audit_add",
]
