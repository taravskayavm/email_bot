import re
import email
from email.message import Message
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from dataclasses import dataclass
from email.header import decode_header, make_header
from email.utils import getaddresses

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
    """Вернуть вложенный оригинал или его заголовки, если есть."""
    for part in msg.walk():
        if part.get_content_type() not in ("message/rfc822", "text/rfc822-headers"):
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
    for part in msg.walk():
        rcpt = part.get("Final-Recipient", "") or part.get("Original-Recipient", "")
        if rcpt:
            return rcpt.split(";")[-1].strip()
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
        match = re.search(r"(?:Final|Original)-Recipient:\s*[^;]+;\s*(\S+)", text, re.I)
        if match:
            return match.group(1)
    return ""


def decoded_header(value: str) -> str:
    try:
        return str(make_header(decode_header(value)))
    except (LookupError, UnicodeError):
        return value


def is_bounce_message(msg: Message) -> bool:
    subject = decoded_header(msg.get("Subject", ""))
    return (
        is_bounce_from(msg.get("From", ""))
        or (
            msg.get_content_type() == "multipart/report"
            and str(msg.get_param("report-type", "")).lower() == "delivery-status"
        )
        or any(p.get_content_type() == "message/delivery-status" for p in msg.walk())
        or bool(re.search(
            r"mail failure|delivery (?:status notification \(failure\)|failure)|"
            r"undelivered mail|ваше сообщение не доставлено",
            subject, re.I,
        ))
    )


@dataclass(frozen=True)
class Bounce:
    recipient: str
    reason: str
    status: str = ""
    action: str = ""
    message_id: str = ""
    uuid: str = ""
    original_date: datetime | None = None


def _addresses(value: str) -> list[str]:
    value = value.rsplit(";", 1)[-1]
    return [a.strip().lower() for _, a in getaddresses([value]) if "@" in a]


def extract_bounces(msg: Message) -> list[Bounce]:
    """Read recipient blocks, including Mail.ru's headers-only original."""
    if not is_bounce_message(msg):
        return []
    original = extract_original_message(msg)
    mid = original.get("Message-ID", "").strip() if original is not None else ""
    uuid = original.get("X-EBOT-UUID", "").strip() if original is not None else ""
    original_date = parse_date_utc(original.get("Date")) if original is not None else None
    subject = decoded_header(msg.get("Subject", "(bounce)"))
    result: dict[str, Bounce] = {}
    has_recipient_blocks = False
    for part in msg.walk():
        if part.get_content_type() != "message/delivery-status":
            continue
        blocks = part.get_payload()
        if not isinstance(blocks, list):
            continue
        for block in blocks:
            recipient = block.get("Final-Recipient") or block.get("Original-Recipient")
            if not recipient:
                continue
            has_recipient_blocks = True
            status = (block.get("Status") or "").strip()
            action = (block.get("Action") or "").strip().lower()
            if action in {"delivered", "relayed", "expanded"} or status.startswith("2."):
                continue
            if action not in {"failed", "delayed"} and not status.startswith(("4.", "5.")):
                continue
            diagnostic = " ".join((block.get("Diagnostic-Code") or "").split())
            for address in _addresses(recipient):
                result[address] = Bounce(address, diagnostic or subject, status, action, mid, uuid, original_date)
    if has_recipient_blocks:
        return list(result.values())

    recipient = extract_recipient_fallback(msg) or msg.get("X-Failed-Recipients", "")
    if not recipient and original is not None and (
        is_bounce_from(msg.get("From", ""))
        or (
            msg.get_content_type() == "multipart/report"
            and str(msg.get_param("report-type", "")).lower() == "delivery-status"
        )
    ):
        # A person's discussion of a mail failure can include an attached
        # original. Its To header alone does not establish a delivery failure.
        recipient = original.get("X-EBOT-Recipient", "") or original.get("To", "")
    # Never use the outer To header: it is the sender's own mailbox.
    return [Bounce(a, subject, message_id=mid, uuid=uuid, original_date=original_date)
            for a in _addresses(recipient)]
