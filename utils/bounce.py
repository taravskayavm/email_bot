"""Read delivery failures without changing mailbox flags."""

import imaplib
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from .bounce_pop3 import sync_bounces_pop3
from .bounce_processing import record_bounce_message
from emailbot.net_imap import get_imap_timeout, imap_connect_ssl

logger = logging.getLogger(__name__)
_SCAN_LOCK = threading.Lock()
_READ_UIDS: dict[tuple, set[bytes]] = {}


def _imap_connect():
    return imap_connect_ssl(os.getenv("IMAP_HOST", "imap.mail.ru"),
                            int(os.getenv("IMAP_PORT", "993")),
                            timeout=get_imap_timeout(15.0))


def try_imap_connect():
    retries = max(1, int(os.getenv("IMAP_RETRIES", "3")))
    for attempt in range(retries):
        try:
            return _imap_connect()
        except OSError:
            if attempt + 1 < retries:
                time.sleep(min(2 ** attempt, 8))
    return None


def _raw_message(data):
    return next((p[1] for p in data or []
                 if isinstance(p, tuple) and isinstance(p[1], bytes)), b"")


def _sync_imap():
    imap = try_imap_connect()
    if imap is None:
        raise OSError("IMAP unavailable")
    try:
        imap.login(os.getenv("EMAIL_ADDRESS"), os.getenv("EMAIL_PASSWORD"))
        typ, _ = imap.select(os.getenv("INBOX_MAILBOX", "INBOX"), readonly=True)
        if typ != "OK":
            raise RuntimeError("Cannot select bounce mailbox")
        days = int(os.getenv("BOUNCE_SINCE_DAYS", "7"))
        # IMAP dates require English month names regardless of Windows locale.
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(days, 0))
        month = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()[cutoff.month - 1]
        criteria = ("SINCE", f"{cutoff.day:02d}-{month}-{cutoff.year}") if days > 0 else ("ALL",)
        # Let the server select DSNs; avoid downloading every incoming message.
        typ, data = imap.uid("search", None, *criteria, "OR", "OR", "FROM",
            '"mailer-daemon"', "FROM", '"postmaster"', "OR", "SUBJECT",
            '"failure"', "SUBJECT", '"undelivered"')
        if typ != "OK":
            raise RuntimeError(f"Bounce mailbox search failed: {data!r}")
        count = 0
        validity = imap.response("UIDVALIDITY")[1]
        scope = (os.getenv("IMAP_HOST"), os.getenv("EMAIL_ADDRESS"),
                 os.getenv("INBOX_MAILBOX", "INBOX"), repr(validity))
        seen = _READ_UIDS.setdefault(scope, set())
        for uid in data[0].split() if data and data[0] else []:
            if uid in seen:
                continue
            typ, parts = imap.uid("fetch", uid, "(BODY.PEEK[])")
            if typ != "OK":
                raise RuntimeError("Bounce message fetch failed")
            raw = _raw_message(parts)
            if raw:
                count += record_bounce_message(raw)
                seen.add(uid)
        return count
    finally:
        try:
            imap.logout()
        except Exception:
            try:
                imap.shutdown()
            except Exception:
                pass


def sync_bounces() -> int:
    """Record new failures; propagate scan errors instead of reporting zero."""
    backend = os.getenv("BOUNCE_FETCH_BACKEND", "auto").strip().lower()
    if backend not in {"auto", "imap", "pop3"}:
        raise ValueError(f"Unknown bounce backend: {backend}")
    if not os.getenv("EMAIL_ADDRESS") or not os.getenv("EMAIL_PASSWORD"):
        raise RuntimeError("Bounce scanner requires mailbox credentials")
    with _SCAN_LOCK:
        if backend == "pop3":
            return sync_bounces_pop3()
        try:
            return _sync_imap()
        except (OSError, imaplib.IMAP4.error):
            if backend == "imap":
                raise
            logger.warning("IMAP bounce scan failed; trying POP3", exc_info=True)
            return sync_bounces_pop3()


def periodic_bounce_check(stop_event):
    interval = max(10, int(os.getenv("BOUNCE_CHECK_INTERVAL_SECONDS", "60")))
    while not stop_event.is_set():
        try:
            count = sync_bounces()
            if count:
                logger.info("Recorded %s new delivery failures", count)
        except Exception:
            logger.exception("Bounce scan failed")
        stop_event.wait(interval)


def scan_bounces() -> int:
    return sync_bounces()
