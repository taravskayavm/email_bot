from __future__ import annotations

import base64
import csv
import hashlib
import imaplib
import json
import logging
import os
import re
import shutil
import time
import uuid
from threading import RLock
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Optional, Set, Tuple

from utils.paths import ensure_parent, expand_path

from .extraction_common import normalize_email as _normalize_email
from .history_key import normalize_history_key
from .tld_registry import tld_of

from utils.tld_utils import allowed_tlds
from emailbot import edit_service
from .suppress_list import is_blocked
from .settings import REPORT_TZ
from utils.email_norm import sanitize_for_send


# --- TZ helpers --------------------------------------------------------------
def ensure_aware_utc(dt: datetime | None) -> datetime:
    """Return a timezone-aware datetime in UTC."""

    value = dt or datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_imap_date_to_utc(date_str: str | None) -> datetime:
    """Best-effort parsing of IMAP date strings into aware UTC datetimes."""

    try:
        dt = parsedate_to_datetime(date_str) if date_str else None
    except Exception:
        dt = None
    return ensure_aware_utc(dt)

_SUPPRESS_ENV = os.getenv("SUPPRESS_LIST_PATH", "var/suppress_list.csv")
_BOUNCE_ENV = os.getenv("BOUNCE_LOG_PATH", "var/bounce_log.csv")
_SYNC_ENV = os.getenv("SYNC_SEEN_EVENTS_PATH", "var/sync_seen_events.csv")
_SOFT_BOUNCE_ENV = os.getenv("SOFT_BOUNCE_PATH", "var/soft_bounces.jsonl")

SUPPRESS_PATH = expand_path(_SUPPRESS_ENV)
BOUNCE_LOG_PATH = expand_path(_BOUNCE_ENV)
SYNC_SEEN_EVENTS_PATH = expand_path(_SYNC_ENV)
SENT_CACHE_FILE = expand_path(os.getenv("SENT_MAILBOX_CACHE", "var/sent_mailbox.cache"))
SENT_LOG_PATH = expand_path(os.getenv("SENT_LOG_PATH", "var/sent_log.csv"))
SOFT_BOUNCE_PATH = expand_path(_SOFT_BOUNCE_ENV)


def _parse_int(value: str | None, default: int) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


SOFT_BOUNCE_RETRY_HOURS = _parse_int(os.getenv("SOFT_BOUNCE_RETRY_HOURS"), 48)
SOFT_BOUNCE_MAX_RETRIES = _parse_int(os.getenv("SOFT_BOUNCE_MAX_RETRIES"), 2)

logger = logging.getLogger(__name__)

_SOFT_BOUNCE_LOCK = RLock()


def _append_jsonl(path: Path, row: dict) -> None:
    ensure_parent(path)
    with _SOFT_BOUNCE_LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def prepare_recipients_for_send(
    recipients: Iterable[str],
) -> Tuple[List[str], Set[str], Dict[str, str]]:
    """Normalise raw recipient strings before attempting delivery."""

    edits = edit_service.load_edits()
    cleaned: List[str] = []
    remap: Dict[str, str] = {}
    origins: Dict[str, List[str]] = {}
    dropped_originals: Set[str] = set()

    for raw in recipients:
        fixed = sanitize_for_send(raw)
        if not fixed:
            if raw:
                dropped_originals.add(raw)
            continue
        # не шлём тем, кто отписался (txt) или попал в suppress CSV (bounce/жалоба)
        if is_blocked(fixed) or is_suppressed(fixed):
            if raw:
                dropped_originals.add(raw)
            else:
                dropped_originals.add(fixed)
            continue
        if fixed != raw:
            remap[raw] = fixed
        cleaned.append(fixed)
        origins.setdefault(fixed, []).append(raw)

    good, dropped_sanitised, mapping = edit_service.apply_edits(edits, cleaned)

    remap.update(mapping)
    for source, target in mapping.items():
        for original in origins.get(source, []):
            remap[original] = target

    dropped: Set[str] = set(dropped_originals)
    for item in dropped_sanitised:
        originals = origins.get(item)
        if originals:
            dropped.update(originals)
        else:
            dropped.add(item)

    return good, dropped, remap


def _format_unsubscribe_target(value: str | None, *, recipient: str | None = None) -> str | None:
    """Return a RFC 2369 compliant URI wrapped in angle brackets.

    Environment variables may contain templates with ``{email}`` placeholders.
    They are substituted on best effort basis; any formatting errors fall back to
    the original value to avoid breaking mail delivery.
    """

    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if recipient:
        try:
            raw = raw.format(email=recipient, EMAIL=recipient)
        except Exception:
            pass
    if not raw:
        return None
    if raw.startswith("<") and raw.endswith(">"):
        return raw
    return f"<{raw}>"


def set_list_unsubscribe_headers(
    msg: EmailMessage,
    *,
    recipient: str | None = None,
    default_mailto: str | None = None,
    default_http: str | None = None,
    default_one_click: bool | None = None,
) -> None:
    """Populate ``List-Unsubscribe`` headers respecting environment overrides."""

    env_mailto = os.getenv("LIST_UNSUB_MAILTO", "").strip()
    env_http = os.getenv("LIST_UNSUB_HTTP", "").strip()
    env_one_click = os.getenv("LIST_UNSUB_ONECLICK")
    if env_mailto:
        mailto = env_mailto
    else:
        mailto = default_mailto
    if env_http:
        http = env_http
    else:
        http = default_http

    def _env_flag(raw: str | None) -> bool | None:
        if raw is None:
            return None
        text = raw.strip()
        if not text:
            return None
        return text == "1"

    flag = _env_flag(env_one_click)
    if flag is None:
        flag = default_one_click if default_one_click is not None else False

    formatted_mailto = _format_unsubscribe_target(mailto, recipient=recipient)
    formatted_http = _format_unsubscribe_target(http, recipient=recipient)

    parts = [p for p in (formatted_mailto, formatted_http) if p]

    if "List-Unsubscribe" in msg:
        del msg["List-Unsubscribe"]
    if "List-Unsubscribe-Post" in msg:
        del msg["List-Unsubscribe-Post"]

    if parts:
        msg["List-Unsubscribe"] = ", ".join(parts)
        if flag and formatted_http:
            msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"


def build_email(
    to_addr: str,
    subject: str,
    body_html: str,
    group_title: str | None = None,
    group_key: str | None = None,
    *,
    override_180d: bool = False,
) -> EmailMessage:
    """Собирает EmailMessage с безопасными заголовками."""

    msg = EmailMessage()
    msg["To"] = to_addr
    msg["Subject"] = subject
    if group_title:
        msg["X-EBOT-Group"] = group_title
    if group_key:
        msg["X-EBOT-Group-Key"] = group_key
    msg.set_content("HTML version required", subtype="plain")
    msg.add_alternative(body_html, subtype="html")
    if override_180d:
        msg["X-EBOT-Override-180d"] = "1"
    set_list_unsubscribe_headers(msg, recipient=to_addr)
    return msg

COMMON_SENT_NAMES = [
    "Sent",
    "Sent Items",
    "Sent Mail",
    "Sent Messages",
    "[Gmail]/Sent Mail",
    "[Google Mail]/Sent Mail",
    "Отправленные",
    "Отправленное",
    "Отправленные письма",
    "Исходящие",
    "Отправленные элементы",
]
_LIST_RE = re.compile(r"\((?P<flags>[^)]*)\)\s+\"(?P<delim>[^\"]+)\"\s+(?P<name>.+)$")


def _read_cached_sent_name() -> str | None:
    try:
        cached = SENT_CACHE_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return cached or None


def _write_cached_sent_name(name: str) -> None:
    try:
        ensure_parent(SENT_CACHE_FILE)
        SENT_CACHE_FILE.write_text(name, encoding="utf-8")
    except Exception:
        pass


class SecretFilter(logging.Filter):
    """Logging filter that masks sensitive values in records."""

    def __init__(self, secrets: List[str]):
        super().__init__()
        self._secrets = [s for s in secrets if s]

    def _mask(self, value: str) -> str:
        for s in self._secrets:
            value = value.replace(s, "***")
        return value

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        if isinstance(record.msg, str):
            record.msg = self._mask(record.msg)
        if isinstance(record.args, dict):
            record.args = {k: self._mask(str(v)) for k, v in record.args.items()}
        elif isinstance(record.args, tuple):
            record.args = tuple(self._mask(str(a)) for a in record.args)
        return True


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_report_tz(dt: datetime) -> datetime:
    tz = ZoneInfo(REPORT_TZ)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _imap_utf7_encode(s: str) -> str:
    """Unicode -> IMAP modified UTF-7."""

    res: list[str] = []
    buf: list[str] = []

    def flush() -> None:
        if not buf:
            return
        chunk = "".join(buf).encode("utf-16-be")
        enc = base64.b64encode(chunk).decode("ascii").rstrip("=").replace("/", ",")
        res.append("&" + enc + "-")
        buf.clear()

    for ch in s:
        code = ord(ch)
        if 0x20 <= code <= 0x7E and ch != "&":
            flush()
            res.append(ch)
        elif ch == "&":
            flush()
            res.append("&-")
        else:
            buf.append(ch)
    flush()
    return "".join(res)


def _imap_utf7_decode(s: str) -> str:
    """IMAP modified UTF-7 -> Unicode."""

    out: list[str] = []
    i = 0
    n = len(s)
    while i < n:
        if s[i] != "&":
            out.append(s[i])
            i += 1
            continue
        j = s.find("-", i)
        if j == -1:
            out.append(s[i:])
            break
        token = s[i + 1 : j]
        if not token:
            out.append("&")
        else:
            b64 = token.replace(",", "/")
            pad = "=" * ((4 - len(b64) % 4) % 4)
            data = base64.b64decode(b64 + pad)
            out.append(data.decode("utf-16-be", errors="strict"))
        i = j + 1
    return "".join(out)


def _normalize_list_line(row: bytes | str) -> str:
    if isinstance(row, bytes):
        return row.decode("utf-8", "ignore")
    return str(row)


def _parse_list_line(line: str) -> tuple[set[str], str, str]:
    """Return (flags, raw_ascii_name, human_name)."""

    match = _LIST_RE.match(line.strip())
    flags: set[str] = set()
    raw_name = ""
    if match:
        flags = set((match.group("flags") or "").split())
        raw_name = (match.group("name") or "").strip()
        if raw_name.startswith('"') and raw_name.endswith('"'):
            raw_name = raw_name[1:-1]
    else:
        parts = line.strip().rsplit(" ", 1)
        raw_name = parts[-1].strip('"') if parts else ""

    decoded = _imap_utf7_decode(raw_name)
    return flags, raw_name, decoded


def _imap_enable_utf8_accept(imap: imaplib.IMAP4) -> None:
    with suppress(Exception):
        imap.enable("UTF8=ACCEPT")


def _try_select_decoded(imap: imaplib.IMAP4, decoded_name: str) -> bool:
    if not decoded_name:
        return False
    with suppress(Exception):
        code, _ = imap.select(decoded_name, readonly=True)
        if code == "OK":
            return True
    encoded = _imap_utf7_encode(decoded_name)
    with suppress(Exception):
        code, _ = imap.select(encoded, readonly=True)
        return code == "OK"
    return False


def _try_select_raw_ascii(imap: imaplib.IMAP4, raw_ascii: str) -> bool:
    if not raw_ascii:
        return False
    with suppress(Exception):
        code, _ = imap.select(raw_ascii, readonly=True)
        return code == "OK"
    return False


REQUIRED_FIELDS = ["key", "email", "last_sent_at", "source", "status"]
LEGACY_MAP = {
    "address": "email",
    "mail": "email",
    "ts": "last_sent_at",
    "timestamp": "last_sent_at",
    "date": "last_sent_at",
    "result": "status",
}


class FileLock:
    """Very small cross-platform file lock."""

    def __init__(self, target: Path):
        self._path = target.with_suffix(target.suffix + ".lock")
        self._fd: int | None = None

    def acquire(self, retries: int = 5, delay: float = 0.1) -> None:
        for i in range(retries):
            try:
                self._fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                return
            except FileExistsError:
                time.sleep(delay * (i + 1))
        raise RuntimeError("could not acquire lock")

    def release(self) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        try:
            os.unlink(self._path)
        except FileNotFoundError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


def _normalize_key(email: str) -> str:
    """Return the canonical form used for history lookups."""

    return normalize_history_key(email)


def canonical_for_history(email: str) -> str:
    """Return canonical key for history deduplication."""

    return _normalize_key(email)


def last_sent_at(email: str) -> Optional[datetime]:
    """Return the last send timestamp in UTC for the given address."""

    key = _normalize_key(email)
    if not key:
        return None
    try:
        from emailbot.services.cooldown import (
            get_last_sent_at as _cooldown_last_sent_at,
        )

        value = _cooldown_last_sent_at(email)
    except Exception:  # pragma: no cover - defensive fallback
        return None
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def detect_sent_folder(imap: imaplib.IMAP4) -> str:
    """Locate the "Sent" folder, honouring overrides and caching the result."""

    cached = _read_cached_sent_name()
    env_sent = (os.getenv("SENT_MAILBOX") or "").strip()

    _imap_enable_utf8_accept(imap)

    if cached and _try_select_decoded(imap, cached):
        return cached

    if env_sent and _try_select_decoded(imap, env_sent):
        if env_sent != cached:
            _write_cached_sent_name(env_sent)
        return env_sent

    with suppress(Exception):
        status, data = imap.list()
        if status == "OK" and data:
            candidates: list[tuple[set[str], str, str]] = []
            for row in data:
                line = _normalize_list_line(row)
                flags, raw_ascii, human = _parse_list_line(line)
                candidates.append((flags, raw_ascii, human))

            for flags, raw_ascii, human in candidates:
                if any(flag.upper() == r"\SENT" for flag in flags):
                    if _try_select_raw_ascii(imap, raw_ascii):
                        to_cache = human or raw_ascii
                        if to_cache != cached:
                            _write_cached_sent_name(to_cache)
                        return to_cache

            known = {name.lower() for name in COMMON_SENT_NAMES}
            for _flags, _raw_ascii, human in candidates:
                if human and human.lower() in known:
                    if _try_select_decoded(imap, human):
                        if human != cached:
                            _write_cached_sent_name(human)
                        return human

    logger.warning(
        "Sent mailbox not selectable, fell back to INBOX (env=%r, cached=%r)",
        env_sent,
        cached,
    )
    return "INBOX"


def append_to_sent(imap, mailbox: str, msg_bytes: bytes) -> tuple[str, object]:
    """Выполняет IMAP APPEND в указанную папку 'Отправленные'.
    Возвращает (status, data).
    """

    try:
        # Флаг прочитанности указываем в стандартном виде
        return imap.append(mailbox, r"(\Seen)", None, msg_bytes)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("APPEND to %s failed: %s", mailbox, e)
        return "NO", e


def _normalize_ts(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        try:
            dt = parsedate_to_datetime(value)
        except Exception:
            try:
                dt = datetime.fromtimestamp(float(value))
            except Exception:
                return value
    if dt is None:
        return value
    return _ensure_report_tz(dt).isoformat()


def ensure_sent_log_schema(path: str) -> List[str]:
    """Ensure ``sent_log.csv`` has the required schema and migrate legacy names."""

    p = Path(path)
    ensure_parent(p)
    if not p.exists():
        with p.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(REQUIRED_FIELDS)
        return list(REQUIRED_FIELDS)

    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        headers = reader.fieldnames or []

    mapped_headers = [LEGACY_MAP.get(h, h) for h in headers]
    all_fields: List[str] = []
    for h in REQUIRED_FIELDS + mapped_headers:
        if h not in all_fields:
            all_fields.append(h)

    migrated: List[Dict[str, str]] = []
    for row in rows:
        new_row: Dict[str, str] = {}
        for k, v in row.items():
            k2 = LEGACY_MAP.get(k, k)
            if k2 == "last_sent_at":
                v = _normalize_ts(v)
            new_row[k2] = v
        for field in all_fields:
            new_row.setdefault(field, "")
        migrated.append(new_row)

    bak = p.with_suffix(p.suffix + ".bak")
    if not bak.exists():
        shutil.copy2(p, bak)
    tmp_rows: Iterable[Dict[str, str]] = migrated
    try:
        _atomic_write(p, tmp_rows, all_fields)
    except Exception:
        if bak.exists():
            shutil.copy2(bak, p)
        raise
    return all_fields


def _atomic_write(
    path: Path, rows: Iterable[Dict[str, str]], headers: List[str]
) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    os.replace(tmp, path)


def load_sent_log(path: Path) -> Dict[str, datetime]:
    data: Dict[str, datetime] = {}
    if not path.exists():
        return data

    tz = ZoneInfo(REPORT_TZ)
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            email = (row.get("email") or "").strip()
            if not email:
                continue
            ts_raw = (row.get("last_sent_at") or "").strip()
            if not ts_raw:
                continue
            try:
                dt = datetime.fromisoformat(ts_raw)
            except Exception:
                continue
            if dt.tzinfo is None:
                dt_local = dt.replace(tzinfo=tz)
            else:
                dt_local = dt.astimezone(tz)
            dt_utc = dt_local.astimezone(timezone.utc)
            key = canonical_for_history(email)
            current = data.get(key)
            if current is None or current < dt_utc:
                data[key] = dt_utc
    return data


def _iter_sent_rows() -> Iterable[Dict[str, str]]:
    path = Path(SENT_LOG_PATH)
    if not path.exists():
        return []
    try:
        with path.open("r", newline="", encoding="utf-8") as f:
            yield from csv.DictReader(f)
    except FileNotFoundError:
        return []
    except Exception:
        logger.debug("failed to read sent log rows", exc_info=True)
        return []


def _content_hash_from_parts(key: str, subject: str, body: str) -> str:
    payload = f"{key}|{subject}|{body}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def upsert_sent_log(
    path: str | Path,
    email: str,
    ts: datetime,
    source: str,
    status: str = "synced",
    extra: Dict[str, str] | None = None,
    *,
    key: str | None = None,
) -> Tuple[bool, bool]:
    """Insert or update ``sent_log`` row using ``key`` for deduplication."""

    p = Path(path)
    ensure_parent(p)
    fieldnames = ensure_sent_log_schema(str(p))
    event_key = (key or "").strip() or canonical_for_history(email)
    tz = ZoneInfo(REPORT_TZ)
    ts_local = _ensure_report_tz(ts)
    ts_utc = ts_local.astimezone(timezone.utc)
    inserted = False
    updated = False
    with FileLock(p):
        rows: List[Dict[str, str]] = []
        if p.exists():
            with p.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        for row in rows:
            row_key = (row.get("key") or "").strip()
            if not row_key:
                row_key = canonical_for_history(row.get("email", ""))
            if row_key != event_key:
                continue
            existing_ts_raw = (row.get("last_sent_at") or "").strip()
            ts_to_store = ts_local
            if existing_ts_raw:
                try:
                    existing_dt = datetime.fromisoformat(existing_ts_raw)
                    if existing_dt.tzinfo is None:
                        existing_local = existing_dt.replace(tzinfo=tz)
                    else:
                        existing_local = existing_dt.astimezone(tz)
                    existing_utc = existing_local.astimezone(timezone.utc)
                    if existing_utc > ts_utc:
                        ts_to_store = existing_local
                except Exception:
                    pass
            row.update(
                {
                    "key": event_key,
                    "email": email.strip(),
                    "last_sent_at": ts_to_store.isoformat(),
                    "source": source,
                    "status": status,
                }
            )
            if extra:
                for k_extra, v_extra in extra.items():
                    row[k_extra] = str(v_extra)
                    if k_extra not in fieldnames:
                        fieldnames.append(k_extra)
            updated = True
            break
        else:
            new_row = {
                "key": event_key,
                "email": email.strip(),
                "last_sent_at": ts_local.isoformat(),
                "source": source,
                "status": status,
            }
            if extra:
                for k_extra, v_extra in extra.items():
                    new_row[k_extra] = str(v_extra)
                    if k_extra not in fieldnames:
                        fieldnames.append(k_extra)
            rows.append(new_row)
            inserted = True

        bak = p.with_suffix(p.suffix + ".bak")
        if p.exists() and not bak.exists():
            shutil.copy2(p, bak)
        try:
            _atomic_write(p, rows, fieldnames)
        except Exception:
            if bak.exists():
                shutil.copy2(bak, p)
            raise
    return inserted, updated


def dedupe_sent_log_inplace(path: str | Path) -> Dict[str, int]:
    p = Path(path)
    ensure_sent_log_schema(str(p))
    rows: List[Dict[str, str]] = []
    if p.exists():
        with p.open(encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    tz = ZoneInfo(REPORT_TZ)
    fieldnames: List[str] = list(REQUIRED_FIELDS)
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    deduped: List[Dict[str, str]] = []
    index_by_key: Dict[str, int] = {}

    def _parse_local(value: str) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=tz)
        return dt.astimezone(tz)

    for row in rows:
        current = dict(row)
        key = (current.get("key") or "").strip()
        if not key:
            key = str(uuid.uuid4())
            current["key"] = key
        idx = index_by_key.get(key)
        if idx is None:
            for fn in fieldnames:
                current.setdefault(fn, "")
            deduped.append(current)
            index_by_key[key] = len(deduped) - 1
            continue
        existing = deduped[idx]
        new_ts = _parse_local(current.get("last_sent_at", ""))
        old_ts = _parse_local(existing.get("last_sent_at", ""))
        if old_ts is None or (new_ts is not None and new_ts.astimezone(timezone.utc) >= old_ts.astimezone(timezone.utc)):
            merged = existing.copy()
            merged.update({fn: current.get(fn, merged.get(fn, "")) for fn in fieldnames})
            if new_ts is not None:
                merged["last_sent_at"] = new_ts.isoformat()
            deduped[idx] = merged

    before = len(rows)
    after = len(deduped)

    bak = p.with_suffix(p.suffix + ".bak")
    if p.exists() and not bak.exists():
        shutil.copy2(p, bak)
    with FileLock(p):
        try:
            _atomic_write(p, deduped, fieldnames)
        except Exception:
            if bak.exists():
                shutil.copy2(bak, p)
            raise
    return {"before": before, "after": after, "removed": before - after}


def load_seen_events(path: Path) -> set[tuple[str, str]]:
    events: set[tuple[str, str]] = set()
    if path.exists():
        with path.open(encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    events.add((row[0], row[1]))
    return events


def save_seen_events(path: Path, events: Iterable[tuple[str, str]]) -> None:
    headers = ["msgid", "key"]
    rows = [dict(msgid=m, key=k) for m, k in events]
    with FileLock(path):
        _atomic_write(path, rows, headers)


def _ensure_headers(p: Path, headers: List[str]):
    new = not p.exists()
    ensure_parent(p)
    f = p.open("a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=headers)
    if new:
        w.writeheader()
    return f, w


def log_sent(email: str, *args, **kwargs):
    """Wrapper around :func:`messaging.log_sent_email`."""

    from . import messaging as _messaging

    return _messaging.log_sent_email(email, *args, **kwargs)


def was_sent_within(email: str, days: int = 180) -> bool:
    """Check recent sends."""

    from . import history_service as _history_service

    return _history_service.was_sent_within_days(email, "", days)


def was_sent_today_same_content(email: str, subject: str, body: str) -> bool:
    """Return True if the same message was sent within the last 24 hours."""

    tz = ZoneInfo(REPORT_TZ)
    subject_norm = subject or ""
    body_norm = body or ""
    start_local = datetime.now(tz) - timedelta(hours=24)
    key = canonical_for_history(email)
    if not key:
        return False
    target_hash = _content_hash_from_parts(key, subject_norm, body_norm)
    for row in _iter_sent_rows():
        status = (row.get("status") or "ok").strip().lower()
        if status not in {"ok", "sent", "success"}:
            continue
        ts_raw = (row.get("last_sent_at") or row.get("ts") or "").strip()
        if not ts_raw:
            continue
        try:
            dt = datetime.fromisoformat(ts_raw)
        except Exception:
            continue
        if dt.tzinfo is None:
            dt_local = dt.replace(tzinfo=tz)
        else:
            dt_local = dt.astimezone(tz)
        if dt_local < start_local:
            continue
        row_key = (row.get("key") or "").strip()
        if not row_key:
            row_key = canonical_for_history(row.get("email", ""))
        if row_key != key:
            continue
        existing_hash = (row.get("content_hash") or row.get("body_hash") or "").strip()
        if existing_hash:
            if existing_hash == target_hash:
                return True
            continue
        row_subject = (row.get("subject") or "").strip()
        row_body = (row.get("body") or "").strip()
        if not row_subject and not row_body:
            continue
        old_hash = _content_hash_from_parts(row_key, row_subject, row_body)
        if old_hash == target_hash:
            return True
    return False


def add_bounce(email: str, code: int | None, msg: str, phase: str) -> None:
    f, w = _ensure_headers(BOUNCE_LOG_PATH, ["ts", "email", "code", "msg", "phase"])
    with f:
        w.writerow(
            {
                "ts": _now(),
                "email": (email or "").lower().strip(),
                "code": code or "",
                "msg": (msg or "")[:500],
                "phase": phase,
            }
        )
    logger.info(
        "bounce recorded",
        extra={
            "event": "bounce",
            "email": (email or "").lower().strip(),
            "code": code,
            "phase": phase,
        },
    )


def log_soft_bounce(
    email: str,
    *,
    reason: str,
    group_code: str,
    chat_id: int,
    template_path: str,
    code: int | None = None,
) -> None:
    """Persist soft-bounce details for scheduled retries."""

    key = _normalize_key(email)
    if not key:
        return
    now = datetime.now(timezone.utc)
    next_retry = now + timedelta(hours=SOFT_BOUNCE_RETRY_HOURS)
    row = {
        "ts": now.isoformat(),
        "email": key,
        "reason": (reason or "")[:300],
        "group": group_code,
        "chat_id": chat_id,
        "template": template_path,
        "next_retry_at": next_retry.isoformat(),
        "retry_count": 0,
        "max_retries": SOFT_BOUNCE_MAX_RETRIES,
        "status": "soft_bounce",
        "code": code,
    }
    _append_jsonl(SOFT_BOUNCE_PATH, row)


def mark_soft_bounce_success(email: str) -> None:
    """Remove pending soft-bounce retries once delivery succeeds."""

    key = _normalize_key(email)
    if not key:
        return
    path = SOFT_BOUNCE_PATH
    with _SOFT_BOUNCE_LOCK:
        if not path.exists():
            return
        try:
            preserved: list[str] = []
            with path.open("r", encoding="utf-8") as src:
                for line in src:
                    try:
                        row = json.loads(line)
                    except Exception:
                        preserved.append(line)
                        continue
                    if _normalize_key(row.get("email", "")) == key and row.get("status") == "soft_bounce":
                        continue
                    preserved.append(json.dumps(row, ensure_ascii=False) + "\n")
        except FileNotFoundError:
            return
        with path.open("w", encoding="utf-8") as dst:
            dst.writelines(preserved)


def _extract_code(code: int | None, msg: str | bytes | None) -> int | None:
    """Best effort extraction of SMTP status code."""

    if code is not None:
        try:
            return int(code)
        except Exception:
            pass
    if msg:
        text = (
            msg.decode("utf-8", "ignore")
            if isinstance(msg, (bytes, bytearray))
            else str(msg)
        )
        m = re.search(r"\b(\d{3})\b", text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def is_hard_bounce(code: int | None, msg: str | bytes | None) -> bool:
    """Return True for permanent delivery failures."""

    icode = _extract_code(code, msg)
    if icode is not None:
        if 500 <= icode < 600:
            return True
        if 400 <= icode < 500:
            return False
    m = (
        (msg or b"").decode("utf-8", "ignore")
        if isinstance(msg, (bytes, bytearray))
        else (msg or "")
    ).lower()
    patterns = [
        "user not found",
        "invalid mailbox",
        "no such user",
        "unknown user",
        "non-local recipient verification failed",
        "recipient address rejected",
        "user is terminated",
        "mailbox unavailable",
        "mailbox disabled",
    ]
    return any(p in m for p in patterns)


def is_soft_bounce(code: int | None, msg: str | bytes | None) -> bool:
    """Return True for temporary delivery failures."""

    if is_hard_bounce(code, msg):
        return False

    icode = _extract_code(code, msg)
    if icode is not None:
        if 400 <= icode < 500:
            return True
        if 500 <= icode < 600:
            return False

    m = (
        (msg or b"").decode("utf-8", "ignore")
        if isinstance(msg, (bytes, bytearray))
        else (msg or "")
    ).lower()
    patterns = [
        "temporary",
        "try again later",
        "greylist",
        "graylist",
        "timed out",
        "timeout",
        "rate limit",
        "too many connections",
        "temporarily deferred",
        "temporarily unavailable",
    ]
    return any(p in m for p in patterns)


def suppress_add(email: str, code: int | None, reason: str) -> None:
    key = _normalize_key(email)
    if not key:
        return
    rows: dict[str, dict] = {}
    if SUPPRESS_PATH.exists():
        with SUPPRESS_PATH.open("r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing = _normalize_key(r.get("email", ""))
                if existing:
                    rows[existing] = r
    rec = rows.get(key)
    if rec:
        rec["last_seen"] = _now()
        rec["hits"] = str(int(rec.get("hits", "1")) + 1)
        rec["code"] = str(code or rec.get("code", ""))
        rec["reason"] = reason or rec.get("reason", "")
    else:
        rec = {
            "email": key,
            "code": str(code or ""),
            "reason": reason or "hard bounce",
            "first_seen": _now(),
            "last_seen": _now(),
            "hits": "1",
        }
    rows[key] = rec
    ensure_parent(SUPPRESS_PATH)
    with SUPPRESS_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["email", "code", "reason", "first_seen", "last_seen", "hits"],
        )
        w.writeheader()
        w.writerows(rows.values())


def is_suppressed(email: str) -> bool:
    key = _normalize_key(email)
    if not key or not SUPPRESS_PATH.exists():
        return False
    with SUPPRESS_PATH.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            existing = _normalize_key(r.get("email", ""))
            if existing and existing == key:
                return True
    return False


def _split_allowed_tlds() -> tuple[set[str], set[str]]:
    allowed_upper = {t.upper() for t in allowed_tlds() if t}
    domestic = {"RU"} & allowed_upper
    generic = allowed_upper - domestic
    return domestic, generic


DOMESTIC_CCTLD, GENERIC_GTLD = _split_allowed_tlds()


def classify_tld(email: str) -> Literal["domestic", "foreign", "generic"]:
    email = (email or "").strip()
    if "@" not in email:
        return "foreign"
    domain = email.split("@", 1)[1]
    tld = tld_of(domain)
    if tld is None:
        return "foreign"
    domestic, generic = _split_allowed_tlds()
    tld_upper = tld.upper()
    if tld_upper in domestic:
        return "domestic"
    if tld_upper in generic:
        return "generic"
    return "foreign"


def is_foreign(email: str) -> bool:
    """Return True if the e-mail has a TLD outside allowed/domestic/generic sets."""
    return classify_tld(email) == "foreign"


__all__ = [
    "SUPPRESS_PATH",
    "BOUNCE_LOG_PATH",
    "SYNC_SEEN_EVENTS_PATH",
    "SOFT_BOUNCE_PATH",
    "REQUIRED_FIELDS",
    "LEGACY_MAP",
    "ensure_aware_utc",
    "ensure_sent_log_schema",
    "canonical_for_history",
    "last_sent_at",
    "upsert_sent_log",
    "dedupe_sent_log_inplace",
    "add_bounce",
    "log_soft_bounce",
    "mark_soft_bounce_success",
    "is_hard_bounce",
    "is_soft_bounce",
    "suppress_add",
    "is_suppressed",
    "parse_imap_date_to_utc",
    "classify_tld",
    "DOMESTIC_CCTLD",
    "GENERIC_GTLD",
    "is_foreign",
    "was_sent_within",
    "was_sent_today_same_content",
    "log_sent",
    "SecretFilter",
]
