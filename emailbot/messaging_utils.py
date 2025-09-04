import csv
import logging
import os
import re
import time
import unicodedata
import shutil
import email.utils
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Iterable, Tuple

SUPPRESS_PATH = Path("/mnt/data/suppress_list.csv")  # e-mail, code, reason, first_seen, last_seen, hits
BOUNCE_LOG_PATH = Path("/mnt/data/bounce_log.csv")   # ts, email, code, msg, phase
SYNC_SEEN_EVENTS_PATH = Path("/mnt/data/sync_seen_events.csv")

logger = logging.getLogger(__name__)


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


_ZERO_WIDTH_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")


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
    """Return canonical key for history deduplication."""

    email = (email or "").strip().strip("'\"")
    email = _ZERO_WIDTH_RE.sub("", email)
    email = unicodedata.normalize("NFKC", email).lower()
    local, sep, domain = email.partition("@")
    if not sep:
        return email
    try:
        domain = domain.encode("idna").decode("ascii")
    except Exception:
        domain = domain.encode("ascii", "ignore").decode("ascii")
    if domain in {"gmail.com", "googlemail.com"}:
        domain = "gmail.com"
        local = local.split("+", 1)[0].replace(".", "")
    return f"{local}@{domain}"


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


def _atomic_write(path: Path, rows: Iterable[Dict[str, str]], headers: List[str]) -> None:
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
        headers = [h for h in headers if h in extra_fields] + [h for h in extra_fields if h not in headers]
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

    from . import messaging as _messaging

    return _messaging.was_sent_within(email, days=days)


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
        extra={"event": "bounce", "email": (email or "").lower().strip(), "code": code, "phase": phase},
    )


def _extract_code(code: int | None, msg: str | bytes | None) -> int | None:
    """Best effort extraction of SMTP status code."""

    if code is not None:
        try:
            return int(code)
        except Exception:
            pass
    if msg:
        text = msg.decode("utf-8", "ignore") if isinstance(msg, (bytes, bytearray)) else str(msg)
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
        (msg or b"")
        .decode("utf-8", "ignore")
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
        (msg or b"")
        .decode("utf-8", "ignore")
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


_ALLOWED_TLDS = {"ru", "com"}

def is_foreign(email: str) -> bool:
    """Return True if the e-mail has a TLD outside the allowed set."""
    if not email:
        return True
    tld = email.rsplit(".", 1)[-1].lower()
    return tld not in _ALLOWED_TLDS


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
    "is_foreign",
    "was_sent_within",
    "log_sent",
    "SecretFilter",
]
