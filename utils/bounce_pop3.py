import os
import poplib
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from email.message import Message
import email

from .send_stats import log_bounce


BOUNCE_FROM = re.compile(r"(mailer-daemon|postmaster)@", re.I)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _extract_original(msg: Message) -> Message | None:
    for part in msg.walk():
        if part.get_content_type() != "message/rfc822":
            continue
        payload = part.get_payload(decode=True)
        if payload:
            try:
                return email.message_from_bytes(payload)
            except Exception:
                continue
        nested = part.get_payload()
        if isinstance(nested, list) and nested:
            obj = nested[0]
            if isinstance(obj, Message):
                return obj
    return None


def _extract_recipient(msg: Message) -> str:
    rcpt = msg.get("Final-Recipient", "") or msg.get("Original-Recipient", "")
    if rcpt:
        return rcpt.split(";")[-1].strip()
    return ""


def sync_bounces_pop3() -> int:
    host = os.getenv("POP3_HOST", "pop.mail.ru")
    port = int(os.getenv("POP3_PORT", "995"))
    timeout = int(os.getenv("POP3_TIMEOUT", "20"))
    user = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    since_days = int(os.getenv("BOUNCE_SINCE_DAYS", "7"))

    cutoff = None
    if since_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    pop = poplib.POP3_SSL(host, port, timeout=timeout)
    try:
        pop.user(user)
        pop.pass_(pwd)

        count = 0
        num = len(pop.list()[1])
        lower = max(1, num - 500)
        for i in range(lower, num + 1):
            resp, lines, octets = pop.retr(i)
            msg = email.message_from_bytes(b"\r\n".join(lines))
            if cutoff is not None:
                msg_dt = _parse_date(msg.get("Date"))
                if msg_dt is not None and msg_dt < cutoff:
                    continue
            if not BOUNCE_FROM.search(msg.get("From", "")):
                continue

            orig = _extract_original(msg)
            uuid = rcpt = mid = ""
            reason = msg.get("Subject", "(bounce)")

            if orig:
                uuid = orig.get("X-EBOT-UUID", "")
                rcpt = orig.get("X-EBOT-Recipient", "") or orig.get("To", "")
                mid = orig.get("Message-ID", "")
            else:
                rcpt = _extract_recipient(msg)

                if not rcpt:
                    for part in msg.walk():
                        if part.get_content_type() not in ("message/delivery-status", "text/plain", "text/rfc822-headers"):
                            continue
                        payload = part.get_payload(decode=True)
                        if not payload:
                            continue
                        try:
                            text = payload.decode(errors="ignore")
                        except AttributeError:
                            text = str(payload)
                        match = re.search(r"Final-Recipient:\s*[^;]+;\s*(\S+)", text, re.I)
                        if match:
                            rcpt = match.group(1)
                            break

            if rcpt:
                log_bounce(rcpt, reason, uuid=uuid, message_id=mid)
                count += 1

        return count
    finally:
        try:
            pop.quit()
        except Exception:
            try:
                pop.close()
            except Exception:
                pass

