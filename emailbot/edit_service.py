# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from utils.dedup import canonical as _canon
from utils.email_clean import preclean_obfuscations
from utils.email_norm import sanitize_for_send

from . import history_store


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
    """Prepare an e-mail for sending without altering the local part."""

    return sanitize_for_send(value or "")


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
        if _as_drop(new_raw):
            if old_key:
                drops.add(old_key)
            continue
        sanitized_new = _norm_email_safe(new_raw)
        if old_key and sanitized_new:
            mapping[old_key] = sanitized_new
    return mapping, drops


def _db_path() -> Path:
    history_store.init_db()
    return history_store._DB_PATH


def load_edits(chat_id: Optional[int] = None) -> Dict[str, Any]:
    """Load stored edits and return mapping/drop structures.

    When ``chat_id`` is ``None`` all edits are returned.  Otherwise edits are
    limited to the specified chat.
    """

    path = _db_path()
    query = "SELECT old_email, new_email FROM edits"
    params: Sequence[Union[int, str]] = ()
    if chat_id is not None:
        query += " WHERE chat_id=?"
        params = (chat_id,)

    with sqlite3.connect(path) as con:
        cur = con.execute(query, params)
        raw_pairs: List[Tuple[str, str]] = [(row[0], row[1]) for row in cur.fetchall()]

    mapping, drops = _build_edit_maps(raw_pairs)
    return {"MAP": mapping, "DROP": drops}


def _normalise_edit_struct(edits: Dict[str, Any]) -> Tuple[Dict[str, str], Set[str]]:
    raw_map = edits.get("MAP") or {}
    raw_drop = edits.get("DROP") or set()

    mapping: Dict[str, str] = {}
    for old_raw, new_raw in dict(raw_map).items():
        old_key = _norm_key(old_raw)
        sanitized_new = _norm_email_safe(new_raw)
        if old_key and sanitized_new:
            mapping[old_key] = sanitized_new

    drops: Set[str] = set()
    for value in raw_drop:
        key = _norm_key(value)
        if key:
            drops.add(key)

    return mapping, drops


def _apply_edits_struct(
    edits: Dict[str, Any], emails: Iterable[str]
) -> Tuple[List[str], Set[str], Dict[str, str]]:
    mapping, drops = _normalise_edit_struct(edits)

    seen: Set[str] = set()
    good: List[str] = []
    dropped: Set[str] = set()
    remap: Dict[str, str] = {}

    for item in emails:
        raw = (item or "").strip()
        if not raw:
            continue
        canon = _norm_key(raw)
        if canon in drops:
            dropped.add(raw)
            continue
        replacement = mapping.get(canon)
        target = replacement if replacement is not None else raw
        final = _norm_email_safe(target)
        if replacement and final and final != raw:
            remap[raw] = final
        if not final:
            dropped.add(raw)
            continue
        if final not in seen:
            seen.add(final)
            good.append(final)

    return good, dropped, remap


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


def apply_edits(
    edits_or_emails: Union[Dict[str, Any], Iterable[str]],
    maybe_emails_or_chat_id: Union[Iterable[str], int, None] = None,
):
    """Apply stored edits or transform addresses with a supplied structure."""

    if isinstance(edits_or_emails, dict):
        emails_iter = maybe_emails_or_chat_id or []
        return _apply_edits_struct(edits_or_emails, emails_iter)  # type: ignore[arg-type]

    emails = list(edits_or_emails)
    chat_id = maybe_emails_or_chat_id
    if not isinstance(chat_id, int):
        raise ValueError("chat_id is required when applying stored edits")

    edits = load_edits(chat_id)
    good, _, _ = _apply_edits_struct(edits, emails)
    return good
