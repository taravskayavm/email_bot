# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from utils.dedup import canonical as _canon
from utils.email_clean import preclean_obfuscations

from . import history_store
from .extraction import normalize_email


_DROP_TOKENS: Set[str] = {"-", "—", "x", "✖", "удалить", "delete", "drop"}

# Маркеры сносок/масок, которые нередко «прилипают» к началу адреса из PDF
# * • · ⁃ † ‡ « » " ( ) [ ] и пробелы
_MASK_CHARS_RX = re.compile(r'^[*\u2022\u00B7\u2043\u2020\u2021"«»()\[\]\s]+')


def _norm_key(value: str) -> str:
    """Return a canonical key for ``value`` suitable for matching edits."""

    cleaned = preclean_obfuscations(value or "")
    # Срезаем лидирующие маркеры сносок у «старого»/«нового» значения
    cleaned = _MASK_CHARS_RX.sub("", cleaned or "")
    return _canon((cleaned or "").strip().lower())


def _norm_email_safe(value: str) -> str:
    """Safely normalise ``value`` to ``local@domain`` when possible."""

    try:
        return normalize_email(value or "").strip()
    except Exception:
        return (value or "").strip().lower()


def _as_drop(value: str) -> bool:
    """Return ``True`` if ``value`` indicates that the address should be removed."""

    return (value or "").strip().lower() in _DROP_TOKENS


def _build_edit_maps(
    raw_pairs: List[Tuple[str, str]]
) -> Tuple[Dict[str, str], Set[str]]:
    """Prepare canonical replacement and drop maps from stored ``raw_pairs``."""

    mapping: Dict[str, str] = {}
    drops: Set[str] = set()
    for old_raw, new_raw in raw_pairs or []:
        old_key = _norm_key(old_raw)
        new_key = _norm_key(new_raw)
        if _as_drop(new_raw):
            if old_key:
                drops.add(old_key)
            continue
        if old_key and new_key:
            mapping[old_key] = new_key
    return mapping, drops


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


def apply_edits(emails: Iterable[str], chat_id: int) -> list[str]:
    path = _db_path()
    with sqlite3.connect(path) as con:
        cur = con.execute(
            "SELECT old_email, new_email FROM edits WHERE chat_id=?", (chat_id,)
        )
        raw_pairs: List[Tuple[str, str]] = [(row[0], row[1]) for row in cur.fetchall()]

    map_canon, drops = _build_edit_maps(raw_pairs)
    seen: Set[str] = set()
    result: List[str] = []
    for item in emails:
        raw = (item or "").strip()
        canon = _norm_key(raw)
        if canon in drops:
            continue
        replacement = map_canon.get(canon)
        target = replacement if replacement is not None else raw
        final = _norm_email_safe(target)
        if final and final not in seen:
            seen.add(final)
            result.append(final)
    return result
