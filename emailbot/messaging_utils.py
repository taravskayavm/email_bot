import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from .messaging import was_sent_within as was_sent_within, log_sent_email as log_sent

SUPPRESS_PATH = Path("/mnt/data/suppress_list.csv")  # e-mail, code, reason, first_seen, last_seen, hits
BOUNCE_LOG_PATH = Path("/mnt/data/bounce_log.csv")   # ts, email, code, msg, phase


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_headers(p: Path, headers: List[str]):
    new = not p.exists()
    p.parent.mkdir(parents=True, exist_ok=True)
    f = p.open("a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=headers)
    if new:
        w.writeheader()
    return f, w


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


def is_hard_bounce(code: int | None, msg: str | bytes | None) -> bool:
    """Return True for permanent delivery failures."""
    if code is not None:
        try:
            icode = int(code)
        except Exception:
            icode = None
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
    icode = None
    if code is not None:
        try:
            icode = int(code)
        except Exception:
            icode = None
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
    "add_bounce",
    "is_hard_bounce",
    "is_soft_bounce",
    "suppress_add",
    "is_suppressed",
    "is_foreign",
    "was_sent_within",
    "log_sent",
]
