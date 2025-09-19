"""Low-level SQLite helpers for send-history registry.
   [EBOT-SQL-COOLDOWN-001]
"""

from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable, Optional, Tuple

from .extraction_common import normalize_email as _normalize_email

logger = logging.getLogger(__name__)

_DB_PATH: Path = Path("var/state.db")
_INITIALIZED = False
_LOCK = Lock()


def _ensure_path(path: Path | str) -> Path:
    path = Path(path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def init_db(path: Path | str | None = None) -> None:
    """Initialise the SQLite database and run legacy migrations."""

    global _DB_PATH, _INITIALIZED
    target = path or _DB_PATH
    resolved = _ensure_path(target)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        conn = sqlite3.connect(resolved)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS send_history(
                    email_norm  TEXT NOT NULL,
                    group_key   TEXT NOT NULL,
                    sent_at_utc TEXT NOT NULL,
                    message_id  TEXT,
                    run_id      TEXT,
                    smtp_result TEXT,
                    PRIMARY KEY (email_norm, group_key, sent_at_utc)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_send_history_last
                  ON send_history(email_norm, group_key, sent_at_utc DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS edits (
                    chat_id   INTEGER NOT NULL,
                    old_email TEXT NOT NULL,
                    new_email TEXT NOT NULL,
                    edited_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_edits_chat_email
                ON edits(chat_id, old_email)
                """
            )
            _run_migrations(conn)
        finally:
            conn.commit()
            conn.close()
        _DB_PATH = resolved
        _INITIALIZED = True


def _ensure_initialized() -> None:
    if not _INITIALIZED:
        init_db(_DB_PATH)


def _connect() -> sqlite3.Connection:
    _ensure_initialized()
    conn = sqlite3.connect(_DB_PATH, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _canonical_email(email: str) -> str:
    email = (email or "").strip()
    if not email:
        return ""
    try:
        return _normalize_email(email)
    except Exception:
        return email.lower()


def _canonical_group(group: str) -> str:
    return (group or "").strip().lower()


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return _ensure_utc(dt).isoformat().replace("+00:00", "Z")


def _parse_datetime(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(dt)


def _prepare_row(
    email: str,
    group: str,
    message_id: str | None,
    sent_at: datetime | None,
    *,
    run_id: str = "",
    smtp_result: str = "ok",
) -> Optional[tuple[str, str, str, str | None, str | None, str | None]]:
    if sent_at is None:
        return None
    email_norm = _canonical_email(email)
    if not email_norm:
        return None
    group_norm = _canonical_group(group)
    msg_norm = (message_id or "").strip() or None
    run_norm = (run_id or "").strip() or None
    result_norm = (smtp_result or "").strip() or None
    return (
        email_norm,
        group_norm,
        _isoformat(sent_at),
        msg_norm,
        run_norm,
        result_norm,
    )


def _insert_history_row(row: tuple[str, str, str, str | None, str | None, str | None]) -> None:
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(
            """
            INSERT OR REPLACE INTO send_history(
                email_norm, group_key, sent_at_utc, message_id, run_id, smtp_result
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            row,
        )
        conn.commit()
    finally:
        conn.close()


def try_reserve_send(
    email_norm: str,
    group_key: str,
    sent_at_utc: datetime,
    *,
    cooldown: timedelta,
    message_id: str = "",
    run_id: str = "",
    smtp_result: str = "pending",
) -> bool:
    """Attempt to register a send while enforcing the cooldown window.

    Returns ``True`` if the record was inserted, ``False`` if an existing
    entry within the cooldown window blocks the reservation.
    """

    sent_at_utc = _ensure_utc(sent_at_utc)
    email_norm = (email_norm or "").strip().lower()
    if not email_norm:
        return False
    group_key = (group_key or "").strip().lower()
    sent_at_iso = _isoformat(sent_at_utc)
    cooldown_seconds = max(int(cooldown.total_seconds()), 0)
    if cooldown_seconds <= 0:
        _insert_history_row(
            (
                email_norm,
                group_key,
                sent_at_iso,
                (message_id or "").strip() or None,
                (run_id or "").strip() or None,
                (smtp_result or "").strip() or None,
            )
        )
        return True

    threshold = _ensure_utc(sent_at_utc - cooldown)
    threshold_iso = _isoformat(threshold)
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        cursor = conn.execute(
            """
            INSERT INTO send_history(
                email_norm, group_key, sent_at_utc, message_id, run_id, smtp_result
            )
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM send_history
                WHERE email_norm = ?
                  AND group_key = ?
                  AND sent_at_utc >= ?
            )
            """,
            (
                email_norm,
                group_key,
                sent_at_iso,
                (message_id or "").strip() or None,
                (run_id or "").strip() or None,
                (smtp_result or "").strip() or None,
                email_norm,
                group_key,
                threshold_iso,
            ),
        )
        conn.commit()
        return cursor.rowcount == 1
    finally:
        conn.close()


def delete_send_record(email_norm: str, group_key: str, sent_at_utc: datetime) -> None:
    """Remove a send record, typically used to roll back a failed attempt."""

    sent_at_utc = _ensure_utc(sent_at_utc)
    email_norm = (email_norm or "").strip().lower()
    if not email_norm:
        return
    group_key = (group_key or "").strip().lower()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(
            """
            DELETE FROM send_history
            WHERE email_norm = ? AND group_key = ? AND sent_at_utc = ?
            """,
            (email_norm, group_key, _isoformat(sent_at_utc)),
        )
        conn.commit()
    finally:
        conn.close()


def record_send(
    email_norm: str,
    group_key: str,
    sent_at_utc: datetime,
    message_id: str = "",
    run_id: str = "",
    smtp_result: str = "",
) -> None:
    email_norm = (email_norm or "").strip().lower()
    if not email_norm:
        return
    group_key = (group_key or "").strip().lower()
    row = (
        email_norm,
        group_key,
        _isoformat(sent_at_utc),
        (message_id or "").strip() or None,
        (run_id or "").strip() or None,
        (smtp_result or "").strip() or None,
    )
    _insert_history_row(row)


def record_sent(email: str, group: str, msg_id: str | None, sent_at: datetime) -> None:
    row = _prepare_row(email, group, msg_id, sent_at, smtp_result="ok")
    if row is None:
        return
    _insert_history_row(row)


def last_send(email_norm: str, group_key: str) -> Optional[datetime]:
    email_norm = (email_norm or "").strip().lower()
    if not email_norm:
        return None
    group_key = (group_key or "").strip().lower()
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT sent_at_utc FROM send_history
            WHERE email_norm=? AND group_key=?
            ORDER BY sent_at_utc DESC
            LIMIT 1
            """,
            (email_norm, group_key),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return _parse_datetime(row[0])


def last_send_any_group(email_norm: str) -> Optional[Tuple[str, datetime]]:
    email_norm = (email_norm or "").strip().lower()
    if not email_norm:
        return None
    conn = _connect()
    try:
        cur = conn.execute(
            """
            SELECT group_key, sent_at_utc FROM send_history
            WHERE email_norm=?
            ORDER BY sent_at_utc DESC
            LIMIT 1
            """,
            (email_norm,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    dt = _parse_datetime(row[1])
    if dt is None:
        return None
    return row[0], dt


def was_sent_within(email: str, group: str, days: int) -> bool:
    if days <= 0:
        return False
    last = get_last_sent(email, group)
    if last is None:
        return False
    now = datetime.now(timezone.utc)
    return last >= now - timedelta(days=days)


def get_last_sent(email: str, group: str) -> Optional[datetime]:
    email_norm = _canonical_email(email)
    if not email_norm:
        return None
    group_norm = _canonical_group(group)
    return last_send(email_norm, group_norm)


def was_sent_within_any_group(email: str, days: int) -> bool:
    if days <= 0:
        return False
    email_norm = _canonical_email(email)
    if not email_norm:
        return False
    info = last_send_any_group(email_norm)
    if not info:
        return False
    _, last = info
    now = datetime.now(timezone.utc)
    return last >= now - timedelta(days=days)


def _legacy_stats_paths() -> Iterable[Path]:
    seen: set[Path] = set()
    candidates = []
    env = os.getenv("SEND_STATS_PATH")
    if env:
        candidates.append(Path(env))
    candidates.extend([Path("var/send_stats.jsonl")])
    for path in candidates:
        if not path:
            continue
        resolved = _ensure_path(path)
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        yield resolved


def _legacy_sent_log_paths() -> Iterable[Path]:
    seen: set[Path] = set()
    env = os.getenv("LEGACY_SENT_LOG_PATH")
    if env:
        seen.add(_ensure_path(Path(env)))
    defaults = [Path("/mnt/data/sent_log.csv"), Path("var/sent_log.csv")]
    for path in list(seen) + defaults:
        resolved = _ensure_path(path)
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        yield resolved


def _migrate_send_stats(conn: sqlite3.Connection) -> None:
    for path in _legacy_stats_paths():
        try:
            with path.open(encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec.get("status") not in {"success", "ok", "sent"}:
                        continue
                    email = rec.get("email") or ""
                    group = rec.get("group") or rec.get("source") or ""
                    ts = rec.get("ts")
                    dt = _parse_datetime(ts)
                    if dt is None:
                        try:
                            dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                        except Exception:
                            dt = datetime.now(timezone.utc)
                    msg_id = rec.get("message_id") or rec.get("msg_id")
                    status = rec.get("status") or "ok"
                    row = _prepare_row(email, group, msg_id, dt, smtp_result=status)
                    if row is None:
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO send_history(
                            email_norm, group_key, sent_at_utc, message_id, run_id, smtp_result
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        row,
                    )
        except FileNotFoundError:
            continue
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("send_stats migration skipped for %s: %s", path, exc)


def _migrate_sent_log(conn: sqlite3.Connection) -> None:
    for path in _legacy_sent_log_paths():
        try:
            with path.open(encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for rec in reader:
                    email = rec.get("email") or rec.get("key") or ""
                    group = rec.get("source") or rec.get("group") or ""
                    ts = rec.get("last_sent_at")
                    dt = _parse_datetime(ts)
                    if dt is None:
                        continue
                    msg_id = rec.get("message_id") or rec.get("msg_id")
                    row = _prepare_row(email, group, msg_id, dt, smtp_result="ok")
                    if row is None:
                        continue
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO send_history(
                            email_norm, group_key, sent_at_utc, message_id, run_id, smtp_result
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        row,
                    )
        except FileNotFoundError:
            continue
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("sent_log migration skipped for %s: %s", path, exc)


def _run_migrations(conn: sqlite3.Connection) -> None:
    try:
        _migrate_send_stats(conn)
        _migrate_sent_log(conn)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.debug("history_store migrations failed: %s", exc)


__all__ = [
    "init_db",
    "try_reserve_send",
    "delete_send_record",
    "record_send",
    "record_sent",
    "was_sent_within",
    "get_last_sent",
    "last_send",
    "last_send_any_group",
    "was_sent_within_any_group",
]
