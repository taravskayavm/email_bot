from __future__ import annotations

import base64
import csv
import email.utils
import imaplib  # noqa: F401
import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Tuple

from .extraction_common import normalize_email as _normalize_email
from .tld_registry import tld_of

from utils.tld_utils import allowed_tlds

SUPPRESS_PATH = Path(
    "/mnt/data/suppress_list.csv"
)  # e-mail, code, reason, first_seen, last_seen, hits
BOUNCE_LOG_PATH = Path("/mnt/data/bounce_log.csv")  # ts, email, code, msg, phase
SYNC_SEEN_EVENTS_PATH = Path("/mnt/data/sync_seen_events.csv")

logger = logging.getLogger(__name__)


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

# A small cache file is used to remember the IMAP folder where
# outgoing messages should be stored.  The file lives alongside this
# module so the path is deterministic and independent of the working
# directory.
SCRIPT_DIR = Path(__file__).resolve().parent
# Name of the file storing the detected "Sent" folder name.
SENT_CACHE_FILE = SCRIPT_DIR / "imap_sent_folder.txt"


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


def _decode_modified_utf7(s: str) -> str:
    res: list[str] = []
    i = 0
    while i < len(s):
        c = s[i]
        if c == "&":
            j = i + 1
            while j < len(s) and s[j] != "-":
                j += 1
            if j == i + 1:
                res.append("&")
            else:
                chunk = s[i + 1 : j].replace(",", "/")
                pad = "=" * (-len(chunk) % 4)
                res.append(base64.b64decode(chunk + pad).decode("utf-16-be", "ignore"))
            i = j + 1
        else:
            res.append(c)
            i += 1
    return "".join(res)


def _encode_modified_utf7(s: str) -> str:
    res: list[str] = []
    for part in re.split(r"([\u0080-\uFFFF]+)", s):
        if not part:
            continue
        if ord(part[0]) < 128:
            res.append(part.replace("&", "&-"))
        else:
            b = (
                base64.b64encode(part.encode("utf-16-be"))
                .decode("ascii")
                .replace("/", ",")
                .rstrip("=")
            )
            res.append("&" + b + "-")
    return "".join(res)


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


def canonical_for_history(email: str) -> str:
    """Return canonical key for history deduplication.

    Historically ``messaging_utils`` implemented its own normalisation rules
    for comparing e‑mail addresses.  The logic now lives in
    :func:`emailbot.extraction_common.normalize_email`; this thin wrapper keeps
    the old function name for backward compatibility.
    """

    return _normalize_email(email)


def detect_sent_folder(imap) -> str:
    r"""Determine the IMAP folder name for "Sent" messages.

    The function tries a cached value first.  If none is found it inspects
    the output of ``imap.list()`` searching for a mailbox flagged with
    ``\Sent``.  If still not found, a list of common folder names is checked.
    The chosen value is stored in :data:`SENT_CACHE_FILE` for future calls.
    """

    # 1) Cached value
    try:
        if SENT_CACHE_FILE.exists():
            cached = SENT_CACHE_FILE.read_text(encoding="utf-8").strip()
            if cached:
                return cached
    except Exception:
        pass

    candidates: List[str] = []
    # 2) Ask the server
    try:
        status, data = imap.list()
        if status == "OK" and data:
            for raw in data:
                # В IMAP имена ящиков в modified UTF-7
                try:
                    line = raw.decode("imap4-utf-7", "ignore")
                except LookupError:
                    line = _decode_modified_utf7(raw.decode("ascii", "ignore"))
                candidates.append(line)
    except Exception:
        candidates = []

    # Prefer a folder explicitly flagged as Sent
    for line in candidates:
        if "\\Sent" in line:
            # Формат: (<flags>) "<sep>" "<mailbox>"
            name = line.rsplit('"', 2)[1] if '"' in line else line.split()[-1]
            try:
                SENT_CACHE_FILE.write_text(name, encoding="utf-8")
            except Exception:
                pass
            return name

    # Common fallbacks (English and Russian variants)
    COMMON = ["Sent", "Отправленные", "Отправленные письма"]
    for line in candidates:
        for name in COMMON:
            if f'"{name}"' in line or line.endswith(name):
                try:
                    SENT_CACHE_FILE.write_text(name, encoding="utf-8")
                except Exception:
                    pass
                return name

    # Last resort
    fallback = "Sent"
    try:
        SENT_CACHE_FILE.write_text(fallback, encoding="utf-8")
    except Exception:
        pass
    return fallback


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
            dt = email.utils.parsedate_to_datetime(value)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            try:
                dt = datetime.fromtimestamp(float(value))
            except Exception:
                return value
    return dt.isoformat()


def ensure_sent_log_schema(path: str) -> List[str]:
    """Ensure ``sent_log.csv`` has the required schema and migrate legacy names."""

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
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
    if path.exists():
        with path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    dt = datetime.fromisoformat(row["last_sent_at"])
                except Exception:
                    continue
                data[row["key"]] = dt
    return data


def upsert_sent_log(
    path: str | Path,
    email: str,
    ts: datetime,
    source: str,
    status: str = "synced",
    extra: Dict[str, str] | None = None,
) -> Tuple[bool, bool]:
    """Insert or update ``sent_log`` row."""

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ensure_sent_log_schema(str(p))
    key = canonical_for_history(email)
    inserted = False
    updated = False
    with FileLock(p):
        rows: List[Dict[str, str]] = []
        if p.exists():
            with p.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        for row in rows:
            if row.get("key") == key:
                existing_ts = row.get("last_sent_at", "")
                try:
                    existing_dt = datetime.fromisoformat(existing_ts)
                except Exception:
                    existing_dt = None
                if existing_dt and ts <= existing_dt:
                    return False, False
                row.update(
                    {
                        "email": email.strip(),
                        "last_sent_at": ts.isoformat(),
                        "source": source,
                        "status": status,
                    }
                )
                if extra:
                    for k, v in extra.items():
                        row[k] = str(v)
                        if k not in fieldnames:
                            fieldnames.append(k)
                updated = True
                break
        else:
            new_row = {
                "key": key,
                "email": email.strip(),
                "last_sent_at": ts.isoformat(),
                "source": source,
                "status": status,
            }
            if extra:
                for k, v in extra.items():
                    new_row[k] = str(v)
                    if k not in fieldnames:
                        fieldnames.append(k)
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
    best: Dict[str, Dict[str, str]] = {}
    for r in rows:
        key = r.get("key") or canonical_for_history(r.get("email", ""))
        try:
            ts = datetime.fromisoformat(r.get("last_sent_at", ""))
        except Exception:
            continue
        current = best.get(key)
        if current is None or datetime.fromisoformat(current["last_sent_at"]) < ts:
            r = dict(r)
            r["key"] = key
            best[key] = r
    before = len(rows)
    after = len(best)
    headers = ["key", "email", "last_sent_at", "source"]
    if best:
        extra_fields = set().union(*(r.keys() for r in best.values()))
        headers = [h for h in headers if h in extra_fields] + [
            h for h in extra_fields if h not in headers
        ]
    bak = p.with_suffix(p.suffix + ".bak")
    if p.exists() and not bak.exists():
        shutil.copy2(p, bak)
    with FileLock(p):
        try:
            _atomic_write(p, best.values(), headers)
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
    p.parent.mkdir(parents=True, exist_ok=True)
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
    email = (email or "").lower().strip()
    rows: dict[str, dict] = {}
    if SUPPRESS_PATH.exists():
        with SUPPRESS_PATH.open("r", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows[r["email"].lower()] = r
    rec = rows.get(email)
    if rec:
        rec["last_seen"] = _now()
        rec["hits"] = str(int(rec.get("hits", "1")) + 1)
        rec["code"] = str(code or rec.get("code", ""))
        rec["reason"] = reason or rec.get("reason", "")
    else:
        rec = {
            "email": email,
            "code": str(code or ""),
            "reason": reason or "hard bounce",
            "first_seen": _now(),
            "last_seen": _now(),
            "hits": "1",
        }
    rows[email] = rec
    SUPPRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with SUPPRESS_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["email", "code", "reason", "first_seen", "last_seen", "hits"],
        )
        w.writeheader()
        w.writerows(rows.values())


def is_suppressed(email: str) -> bool:
    email = (email or "").lower().strip()
    if not email or not SUPPRESS_PATH.exists():
        return False
    with SUPPRESS_PATH.open("r", newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("email", "").lower().strip() == email:
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
    "REQUIRED_FIELDS",
    "LEGACY_MAP",
    "ensure_sent_log_schema",
    "canonical_for_history",
    "upsert_sent_log",
    "dedupe_sent_log_inplace",
    "add_bounce",
    "is_hard_bounce",
    "is_soft_bounce",
    "suppress_add",
    "is_suppressed",
    "classify_tld",
    "DOMESTIC_CCTLD",
    "GENERIC_GTLD",
    "is_foreign",
    "was_sent_within",
    "log_sent",
    "SecretFilter",
]
