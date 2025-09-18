# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from . import history_store


def _db_path() -> Path:
    history_store.init_db()
    return history_store._DB_PATH


def save_edit(
    chat_id: int, old_email: str, new_email: str, when: datetime | None = None
) -> None:
    path = _db_path()
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO edits(chat_id, old_email, new_email, edited_at) VALUES (?, ?, ?, ?)",
            (chat_id, old_email, new_email, (when or datetime.now()).isoformat()),
        )
        con.commit()


def list_edits(chat_id: int) -> list[tuple[str, str, str]]:
    path = _db_path()
    with sqlite3.connect(path) as con:
        cur = con.execute(
            "SELECT old_email, new_email, edited_at FROM edits WHERE chat_id=? ORDER BY edited_at DESC",
            (chat_id,),
        )
        return list(cur.fetchall())


def clear_edits(chat_id: int) -> None:
    path = _db_path()
    with sqlite3.connect(path) as con:
        con.execute("DELETE FROM edits WHERE chat_id=?", (chat_id,))
        con.commit()


def apply_edits(emails: list[str], chat_id: int) -> list[str]:
    path = _db_path()
    mapping = {}
    with sqlite3.connect(path) as con:
        cur = con.execute(
            "SELECT old_email, new_email FROM edits WHERE chat_id=?", (chat_id,)
        )
        mapping = {row[0].lower(): row[1] for row in cur.fetchall()}
    out = []
    for e in emails:
        out.append(mapping.get(e.lower(), e))
    return out
