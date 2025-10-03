import os, email, time
from datetime import datetime, timedelta, timezone
from .send_stats import log_bounce
from .bounce_pop3 import sync_bounces_pop3
from .bounce_common import (
    is_bounce_from,
    extract_original_message,
    extract_recipient_fallback,
)

from emailbot.net_imap import get_imap_timeout, imap_connect_ssl

BOUNCE_SINCE_DAYS = int(os.getenv("BOUNCE_SINCE_DAYS","7"))
INBOX_MAILBOX = os.getenv("INBOX_MAILBOX","INBOX")
IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_PORT = int(os.getenv("IMAP_PORT","993"))
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
IMAP_TIMEOUT = get_imap_timeout(15.0)
IMAP_RETRIES = int(os.getenv("IMAP_RETRIES","3"))


def _imap_connect():
    return imap_connect_ssl(IMAP_HOST, IMAP_PORT, timeout=IMAP_TIMEOUT)

def try_imap_connect():
    for attempt in range(IMAP_RETRIES):
        try:
            return _imap_connect()
        except TimeoutError:
            time.sleep(min(2 ** attempt, 8))
            continue
        except ConnectionRefusedError:
            time.sleep(min(2 ** attempt, 8))
            continue
        except OSError as e:
            if getattr(e, "winerror", None) == 10061 or "timed out" in str(e).lower():
                time.sleep(min(2 ** attempt, 8))
                continue
            raise
    return None

def sync_bounces():
    """Сканирует INBOX, находит bounce, логирует их в send_stats как status='bounce'."""
    imap = try_imap_connect()
    if imap is None:
        backend = os.getenv("BOUNCE_FETCH_BACKEND", "auto").lower()
        if backend in ("auto", "pop3"):
            return sync_bounces_pop3()
        raise RuntimeError("IMAP unavailable and POP3 fallback disabled")

    imap.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    imap.select(INBOX_MAILBOX)

    since = (datetime.now(timezone.utc) - timedelta(days=BOUNCE_SINCE_DAYS)).strftime("%d-%b-%Y")
    typ, data = imap.search(None, 'SINCE', since)
    if typ != "OK":
        imap.logout()
        return 0

    count = 0
    uids = data[0].split() if data and data[0] else []
    for uid in uids:
        typ, msgd = imap.fetch(uid, '(RFC822)')
        if typ != "OK" or not msgd:
            continue
        m = email.message_from_bytes(msgd[0][1])
        if not is_bounce_from(m.get('From','')):
            continue

        orig = extract_original_message(m)

        if not orig:
            # fallback: иногда поле Diagnostic-Code в тексте даёт получателя
            rcpt = extract_recipient_fallback(m)
            if rcpt:
                log_bounce(rcpt, m.get('Subject','(bounce)'))
                count += 1
            continue

        uuid = orig.get('X-EBOT-UUID','')
        rcpt = orig.get('X-EBOT-Recipient','') or orig.get('To','')
        mid  = orig.get('Message-ID','')
        reason = m.get('Subject','(bounce)')
        if rcpt:
            log_bounce(rcpt, reason, uuid=uuid, message_id=mid)
            count += 1

    imap.logout()
    return count


def scan_bounces() -> int:
    """Backward compatible wrapper around :func:`sync_bounces`."""

    return sync_bounces()
