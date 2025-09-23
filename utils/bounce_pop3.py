import os
import poplib
from datetime import datetime, timedelta, timezone
import email

from .send_stats import log_bounce
from .bounce_common import (
    is_bounce_from,
    extract_original_message,
    extract_recipient_fallback,
    parse_date_utc,
)


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
                msg_dt = parse_date_utc(msg.get("Date"))
                if msg_dt is not None and msg_dt < cutoff:
                    continue
            if not is_bounce_from(msg.get("From", "")):
                continue

            orig = extract_original_message(msg)
            uuid = rcpt = mid = ""
            reason = msg.get("Subject", "(bounce)")

            if orig:
                uuid = orig.get("X-EBOT-UUID", "")
                rcpt = orig.get("X-EBOT-Recipient", "") or orig.get("To", "")
                mid = orig.get("Message-ID", "")
            else:
                rcpt = extract_recipient_fallback(msg)

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
