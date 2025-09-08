import smtplib
from typing import Iterable
from email.message import EmailMessage

TIMEOUT = 15


def send_messages(messages: Iterable[EmailMessage], user: str, password: str, host: str) -> None:
    """Send multiple e-mails over a single SMTP connection.

    Any failure resets the session so that the next message does not get
    a mysterious ``503 sender already given`` error.
    """
    with smtplib.SMTP(host, 587, timeout=TIMEOUT) as smtp:
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
