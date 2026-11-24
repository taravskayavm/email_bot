import os
import poplib
from datetime import datetime, timedelta, timezone
import email

from .bounce_processing import record_bounce_message
from .bounce_common import (
    is_bounce_message,
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
            # POP3 has no server-side sender/subject search. Read only headers
            # until the message is identified as a delivery-report candidate.
            # If TOP is unsupported, let the scan fail visibly rather than
            # falling back to downloading ordinary mail bodies.
            resp, lines, octets = pop.top(i, 0)
            msg = email.message_from_bytes(b"\r\n".join(lines))
            if cutoff is not None:
                msg_dt = parse_date_utc(msg.get("Date"))
                if msg_dt is not None and msg_dt < cutoff:
                    continue
            if not is_bounce_message(msg):
                continue

            resp, lines, octets = pop.retr(i)
            count += record_bounce_message(b"\r\n".join(lines))

        return count
    finally:
        try:
            pop.quit()
        except Exception:
            try:
                pop.close()
            except Exception:
                pass
