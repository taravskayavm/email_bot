import os
import smtplib
from typing import Iterable
from email.message import EmailMessage

PORT = int(os.getenv("SMTP_PORT", "587"))
USE_SSL = os.getenv("SMTP_SSL", "0") == "1"
try:
    TIMEOUT = float(os.getenv("SMTP_TIMEOUT", "30"))
except Exception:
    TIMEOUT = 30.0


def send_messages(messages: Iterable[EmailMessage], user: str, password: str, host: str) -> None:
    """Send multiple e-mails over a single SMTP connection.

    Any failure resets the session so that the next message does not get
    a mysterious ``503 sender already given`` error.
    """
    if USE_SSL:
        with smtplib.SMTP_SSL(host, PORT, timeout=TIMEOUT) as smtp:
            smtp.ehlo()
            smtp.login(user, password)
            for msg in messages:
                try:
                    smtp.send_message(msg)
                except Exception:
                    try:
                        smtp.rset()
                    except Exception:
                        pass
                    raise
    else:
        with smtplib.SMTP(host, PORT, timeout=TIMEOUT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(user, password)
            for msg in messages:
                try:
                    smtp.send_message(msg)
                except Exception:
                    try:
                        smtp.rset()
                    except Exception:
                        pass
                    raise
