"""SQLite-backed storage for send history."""

from __future__ import annotations

import csv
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Iterable

from .extraction_common import normalize_email as _normalize_email

logger = logging.getLogger(__name__)

_DB_PATH: Path = Path("var/state.db")
_INITIALIZED = False
_LOCK = Lock()


def _ensure_path(path: Path) -> Path:
    path = Path(path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def init_db(path: Path = Path("var/state.db")) -> None:
    """Initialise the SQLite database and run legacy migrations."""

    global _DB_PATH, _INITIALIZED
    resolved = _ensure_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        conn = sqlite3.connect(resolved)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sent (
                  email TEXT NOT NULL,
                  grp   TEXT NOT NULL,
                  msg_id TEXT,
                  sent_at TEXT NOT NULL,
                  PRIMARY KEY (email, grp, sent_at)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sent_email_grp
                ON sent(email, grp)
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


def _open() -> sqlite3.Connection:
    _ensure_initialized()
    conn = sqlite3.connect(_DB_PATH)
    return conn


def _canonical_email(email: str) -> str:
    email = (email or "").strip()
    if not email:
        return ""
    try:
        return _normalize_email(email)
    except Exception:
        return email.lower()


def _canonical_group(grp: str) -> str:
    return (grp or "").strip().lower()


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return _ensure_utc(dt).isoformat()


def _parse_datetime(value) -> datetime | None:
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
    email: str, grp: str, msg_id: str | None, sent_at: datetime | None
) -> tuple[str, str, str | None, str] | None:
    if sent_at is None:
        return None
    email_norm = _canonical_email(email)
    if not email_norm:
        return None
    grp_norm = _canonical_group(grp)
    msg_norm = (msg_id or "").strip() or None
    return email_norm, grp_norm, msg_norm, _isoformat(sent_at)


def record_sent(email: str, grp: str, msg_id: str | None, sent_at: datetime) -> None:
    """Persist a new send event."""

    row = _prepare_row(email, grp, msg_id, sent_at)
    if row is None:
        return
    with _open() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO sent(email, grp, msg_id, sent_at) VALUES (?, ?, ?, ?)",
            row,
        )
        conn.commit()


def was_sent_within(email: str, grp: str, days: int) -> bool:
    """Return ``True`` if ``email`` was sent to within ``days`` days for ``grp``."""

    if days <= 0:
        return False
    email_norm = _canonical_email(email)
    if not email_norm:
        return False
    grp_norm = _canonical_group(grp)
    now = datetime.now(timezone.utc)
    cutoff_iso = _isoformat(now - timedelta(days=days))
    with _open() as conn:
        cur = conn.execute(
            "SELECT 1 FROM sent WHERE email=? AND grp=? AND sent_at >= ? LIMIT 1",
            (email_norm, grp_norm, cutoff_iso),
        )
        return cur.fetchone() is not None


def get_last_sent(email: str, grp: str) -> datetime | None:
    """Return the most recent send timestamp for ``email``/``grp`` if present."""

    email_norm = _canonical_email(email)
    if not email_norm:
        return None
    grp_norm = _canonical_group(grp)
    with _open() as conn:
        cur = conn.execute(
            "SELECT sent_at FROM sent WHERE email=? AND grp=? ORDER BY sent_at DESC LIMIT 1",
            (email_norm, grp_norm),
        )
        row = cur.fetchone()
    if not row:
        return None
    value = row[0]
    return _parse_datetime(value)


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
                    grp = rec.get("group") or rec.get("source") or ""
                    ts = rec.get("ts")
                    dt = _parse_datetime(ts)
                    if dt is None:
                        try:
                            dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                        except Exception:
                            dt = datetime.now(timezone.utc)
                    msg_id = rec.get("message_id") or rec.get("msg_id")
                    row = _prepare_row(email, grp, msg_id, dt)
                    if row is None:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO sent(email, grp, msg_id, sent_at) VALUES (?, ?, ?, ?)",
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
                for row in reader:
                    email = row.get("email") or row.get("key") or ""
                    grp = row.get("source") or row.get("group") or ""
                    ts = row.get("last_sent_at")
                    dt = _parse_datetime(ts)
                    if dt is None:
                        continue
                    msg_id = row.get("message_id") or row.get("msg_id")
                    prepared = _prepare_row(email, grp, msg_id, dt)
                    if prepared is None:
                        continue
                    conn.execute(
                        "INSERT OR REPLACE INTO sent(email, grp, msg_id, sent_at) VALUES (?, ?, ?, ?)",
                        prepared,
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
    "record_sent",
    "was_sent_within",
    "get_last_sent",
]
