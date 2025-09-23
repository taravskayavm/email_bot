import re
import email
from email.message import Message
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

BOUNCE_FROM_RE = re.compile(r"(mailer-daemon|postmaster)@", re.I)


def is_bounce_from(value: str | None) -> bool:
    return bool(value and BOUNCE_FROM_RE.search(value))


def parse_date_utc(value: str | None) -> datetime | None:
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


def extract_original_message(msg: Message) -> Message | None:
    """Вернуть вложенный оригинал (message/rfc822), если есть."""
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


def extract_recipient_fallback(msg: Message) -> str:
    """Попробовать достать получателя из текстовой части (Final-Recipient…)."""
    rcpt = msg.get("Final-Recipient", "") or msg.get("Original-Recipient", "")
    if rcpt:
        return rcpt.split(";")[-1].strip()
    for part in msg.walk():
        if part.get_content_type() not in (
            "message/delivery-status",
            "text/plain",
            "text/rfc822-headers",
        ):
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
            return match.group(1)
    return ""
