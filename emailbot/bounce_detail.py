"""Bounce detail extraction and persistence utilities."""

from __future__ import annotations

import csv
import os
from datetime import datetime
from email import message_from_bytes
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from .settings import REPORT_TZ, BOUNCE_DETAIL_PATH

FIELDS = [
    "ts_local",
    "email",
    "dsn_status",
    "dsn_action",
    "diagnostic",
    "subject",
    "message_id",
]


def ensure_bounce_detail_schema() -> None:
    """Create CSV with header if it does not yet exist."""

    if os.path.exists(BOUNCE_DETAIL_PATH):
        return
    with open(BOUNCE_DETAIL_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDS)


def _extract_dsn_part(msg: Message) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return ``(status, action, diagnostic)`` from DSN payload if available."""

    if msg.get_content_type() == "message/delivery-status":
        status = msg.get("Status")
        action = msg.get("Action")
        diagnostic = msg.get("Diagnostic-Code") or msg.get("X-Postfix-Sender-Notes")
        return status, action, diagnostic
    if msg.is_multipart():
        for part in msg.get_payload():
            status, action, diagnostic = _extract_dsn_part(part)
            if status or action or diagnostic:
                return status, action, diagnostic
    return None, None, None


def _first_to_address(msg: Message) -> Optional[str]:
    candidates = msg.get_all("Final-Recipient") or msg.get_all("Original-Recipient")
    if not candidates:
        candidates = msg.get_all("To")
    if not candidates:
        return None
    addresses = [addr for _, addr in getaddresses(candidates) if addr]
    if not addresses:
        return None
    return addresses[0].lower()


def parse_and_write_bounce_detail(raw_bytes: bytes) -> None:
    """Parse DSN payload and append details to ``BOUNCE_DETAIL_PATH``."""

    ensure_bounce_detail_schema()
    tz = ZoneInfo(REPORT_TZ)
    try:
        msg = message_from_bytes(raw_bytes)
    except Exception:
        return
    subject = (msg.get("Subject") or "").strip()
    message_id = (msg.get("Message-Id") or "").strip()
    ts_local = datetime.now(tz)
    date_header = msg.get("Date")
    if date_header:
        try:
            parsed = parsedate_to_datetime(date_header)
            ts_local = parsed.replace(tzinfo=tz) if parsed.tzinfo is None else parsed.astimezone(tz)
        except Exception:
            pass
    status, action, diagnostic = _extract_dsn_part(msg)
    address = _first_to_address(msg) or ""
    row = [
        ts_local.isoformat(timespec="seconds"),
        address,
        (status or "").strip(),
        (action or "").strip(),
        (diagnostic or "").strip()[:500],
        subject[:200],
        message_id,
    ]
    try:
        with open(BOUNCE_DETAIL_PATH, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
    except Exception:
        pass


__all__ = ["parse_and_write_bounce_detail", "ensure_bounce_detail_schema"]
