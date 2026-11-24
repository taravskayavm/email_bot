"""Persist the same delivery failure once, across IMAP and POP3 scans."""

import email
import hashlib
import re

from .bounce_common import extract_bounces, parse_date_utc
from .send_stats import log_bounce


def record_bounce_message(raw: bytes) -> int:
    from emailbot.messaging_utils import add_bounce, is_hard_bounce, suppress_add

    msg = email.message_from_bytes(raw)
    received = parse_date_utc(msg.get("Date"))
    identity = msg.get("Message-ID", "").strip() or hashlib.sha256(raw).hexdigest()
    count = 0
    for bounce in extract_bounces(msg):
        code_match = re.search(r"\b([45]\d\d)(?:[ -]|$)", bounce.reason)
        code = int(code_match[1]) if code_match else None
        bounce_id = hashlib.sha256(f"{identity}:{bounce.recipient}".encode()).hexdigest()
        extra = {"dsn_status": bounce.status, "dsn_action": bounce.action,
                 "recipient": bounce.recipient}
        if received:
            extra["ts"] = received.isoformat()
        if bounce.original_date:
            extra["original_sent_at"] = bounce.original_date.isoformat()
        if not log_bounce(bounce.recipient, bounce.reason, bounce.uuid, bounce.message_id,
                          bounce_id=bounce_id, extra=extra):
            continue
        add_bounce(bounce.recipient, code, bounce.reason, phase="delivery")
        if bounce.status.startswith("5.") or (
            not bounce.status.startswith("4.") and is_hard_bounce(code, bounce.reason)
        ):
            suppress_add(bounce.recipient, code, bounce.reason)
        count += 1
    return count
