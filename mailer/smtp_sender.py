import logging
import os
import smtplib
import time
from typing import Iterable
from email.message import EmailMessage
from smtplib import SMTPResponseException

from utils.send_stats import log_error, log_success

PORT = int(os.getenv("SMTP_PORT", "587"))
USE_SSL = os.getenv("SMTP_SSL", "0") == "1"
logger = logging.getLogger(__name__)
try:
    TIMEOUT = float(os.getenv("SMTP_TIMEOUT", "30"))
except Exception:
    TIMEOUT = 30.0


def send_messages(messages: Iterable[EmailMessage], user: str, password: str, host: str) -> None:
    """Send multiple e-mails over a single SMTP connection.

    Any failure resets the session so that the next message does not get
    a mysterious ``503 sender already given`` error.
    """
    def _send_one(msg):
        try:
            smtp.send_message(msg, from_addr=os.getenv("EMAIL_ADDRESS", None))
            try:
                log_success(
                    msg.get("To", ""),
                    msg.get("X-EBOT-Group", ""),
                    extra={
                        "uuid": msg.get("X-EBOT-UUID", ""),
                        "message_id": msg.get("Message-ID", ""),
                    },
                )
            except Exception:
                pass
            return True, None
        except SMTPResponseException as e:
            code = e.smtp_code
            text = e.smtp_error.decode() if isinstance(e.smtp_error, bytes) else e.smtp_error
            if code == 503:
                try:
                    smtp.rset()
                except Exception:
                    pass
                time.sleep(1)
                try:
                    smtp.send_message(msg, from_addr=os.getenv("EMAIL_ADDRESS", None))
                    try:
                        log_success(
                            msg.get("To", ""),
                            msg.get("X-EBOT-Group", ""),
                            extra={
                                "uuid": msg.get("X-EBOT-UUID", ""),
                                "message_id": msg.get("Message-ID", ""),
                            },
                        )
                    except Exception:
                        pass
                    return True, None
                except Exception as e2:
                    e = e2
            try:
                log_error(
                    msg.get("To", ""),
                    msg.get("X-EBOT-Group", ""),
                    f"{code} {text}",
                    extra={
                        "uuid": msg.get("X-EBOT-UUID", ""),
                        "message_id": msg.get("Message-ID", ""),
                    },
                )
            except Exception:
                pass
            return False, e
        except Exception as e:
            try:
                log_error(
                    msg.get("To", ""),
                    msg.get("X-EBOT-Group", ""),
                    repr(e),
                    extra={
                        "uuid": msg.get("X-EBOT-UUID", ""),
                        "message_id": msg.get("Message-ID", ""),
                    },
                )
            except Exception:
                pass
            return False, e

    if USE_SSL:
        with smtplib.SMTP_SSL(host, PORT, timeout=TIMEOUT) as smtp:
            smtp.ehlo()
            smtp.login(user, password)
            for msg in messages:
                if "From" not in msg:
                    from_name = os.getenv("EMAIL_FROM_NAME", "")
                    from_addr = os.getenv("EMAIL_ADDRESS", "")
                    if from_addr:
                        display = (from_name or from_addr).rstrip(". ").rstrip(" ")
                        msg["From"] = f"{display} <{from_addr}>"
                try:
                    logger.info("SMTP send From=%r To=%r", msg.get("From"), msg.get("To"))
                except Exception:
                    pass
                ok, err = _send_one(msg)
                if not ok:
                    try:
                        smtp.rset()
                    except Exception:
                        pass
                    raise err
    else:
        with smtplib.SMTP(host, PORT, timeout=TIMEOUT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(user, password)
            for msg in messages:
                if "From" not in msg:
                    from_name = os.getenv("EMAIL_FROM_NAME", "")
                    from_addr = os.getenv("EMAIL_ADDRESS", "")
                    if from_addr:
                        display = (from_name or from_addr).rstrip(". ").rstrip(" ")
                        msg["From"] = f"{display} <{from_addr}>"
                try:
                    logger.info("SMTP send From=%r To=%r", msg.get("From"), msg.get("To"))
                except Exception:
                    pass
                ok, err = _send_one(msg)
                if not ok:
                    try:
                        smtp.rset()
                    except Exception:
                        pass
                    raise err
