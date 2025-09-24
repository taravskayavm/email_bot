from __future__ import annotations

import json
import sqlite3
import time
from typing import List

from . import history_store


def _db() -> sqlite3.Connection:
    """Return a SQLite connection ensuring the history database exists."""

    history_store.init_db()
    return sqlite3.connect(history_store._DB_PATH)


def _init() -> None:
    with _db() as con:
        con.execute(
            """
        CREATE TABLE IF NOT EXISTS sections_prefs (
            chat_id    INTEGER NOT NULL,
            domain     TEXT    NOT NULL,
            prefixes   TEXT    NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (chat_id, domain)
        )
        """
        )
        con.commit()


_init()


def save_sections_for_domain(chat_id: int, domain: str, prefixes: List[str]) -> None:
    """Persist section prefixes for the given chat/domain pair."""

    if not domain or not prefixes:
        return
    payload = json.dumps(list(dict.fromkeys(prefixes)))
    with _db() as con:
        con.execute(
            "REPLACE INTO sections_prefs(chat_id, domain, prefixes, updated_at) VALUES (?,?,?,?)",
            (int(chat_id), domain.lower().strip(), payload, int(time.time())),
        )
        con.commit()


def get_last_sections_for_domain(chat_id: int, domain: str) -> List[str]:
    """Return previously stored section prefixes for ``chat_id``/``domain``."""

    if not domain:
        return []
    with _db() as con:
        cur = con.execute(
            "SELECT prefixes FROM sections_prefs WHERE chat_id=? AND domain=?",
            (int(chat_id), domain.lower().strip()),
        )
        row = cur.fetchone()
        if not row:
            return []
        try:
            data = json.loads(row[0] or "[]")
        except Exception:
            return []
        result: list[str] = []
        for item in data:
            if isinstance(item, str) and item.startswith("/"):
                result.append(item)
        return result
