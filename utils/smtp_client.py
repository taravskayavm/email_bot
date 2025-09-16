import logging
import os
import smtplib
import time
from email.message import EmailMessage
from typing import Optional


logger = logging.getLogger(__name__)


class RobustSMTP:
    """SMTP клиент с автоматическим переподключением."""

    def __init__(self) -> None:
        self.host = os.getenv("SMTP_HOST", "smtp.mail.ru")
        try:
            self.port = int(os.getenv("SMTP_PORT", "465"))
        except Exception:
            self.port = 465
        self.ssl = os.getenv("SMTP_SSL", "1") == "1"
        try:
            self.timeout = int(os.getenv("SMTP_TIMEOUT", "45"))
        except Exception:
            self.timeout = 45
        self.user = os.getenv("EMAIL_ADDRESS")
        self.pwd = os.getenv("EMAIL_PASSWORD")
        self._smtp: Optional[smtplib.SMTP] = None
        self._logged_config = False

    def _log_config(self) -> None:
        if not self._logged_config:
            logger.info(
                "SMTP connect config host=%s port=%s ssl=%s timeout=%s",
                self.host,
                self.port,
                self.ssl,
                self.timeout,
            )
            self._logged_config = True

    def connect(self) -> None:
        self._log_config()
        if self.ssl:
            smtp: smtplib.SMTP = smtplib.SMTP_SSL(
                self.host, self.port, timeout=self.timeout
            )
            smtp.ehlo()
        else:
            smtp = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
        smtp.login(self.user, self.pwd)
        self._smtp = smtp

    def ensure(self) -> None:
        try:
            if self._smtp is None:
                self.connect()
            else:
                self._smtp.noop()
        except Exception:
            try:
                self.close()
            except Exception:
                pass
            self.connect()

    def send(self, msg: EmailMessage):
        self.ensure()
        assert self._smtp is not None
        return self._smtp.send_message(msg)

    def close(self) -> None:
        if self._smtp is not None:
            try:
                self._smtp.quit()
            finally:
                self._smtp = None


def send_with_retry(
    smtp: RobustSMTP, msg: EmailMessage, retries: int = 3, backoff: float = 1.0
):
    for attempt in range(retries):
        try:
            return smtp.send(msg)
        except (
            smtplib.SMTPServerDisconnected,
            smtplib.SMTPConnectError,
            TimeoutError,
        ):
            if attempt == retries - 1:
                raise
            time.sleep(backoff)
            backoff *= 2
            smtp.ensure()


__all__ = ["RobustSMTP", "send_with_retry"]
