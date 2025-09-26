import logging
import os
import smtplib
import ssl
import time
from collections import deque
from email.message import EmailMessage
from typing import Deque, List, Optional


logger = logging.getLogger(__name__)


def _parse_limit(env_name: str, default: int) -> int:
    try:
        value = int(os.getenv(env_name, str(default)))
    except Exception:
        return default
    return max(0, value)


MAX_PER_MIN = _parse_limit("SMTP_MAX_PER_MIN", 20)
MAX_PER_HOUR = _parse_limit("SMTP_MAX_PER_HOUR", 200)
_TS_MIN: Deque[float] = deque()
_TS_HOUR: Deque[float] = deque()


def _throttle_block() -> None:
    if MAX_PER_MIN <= 0 and MAX_PER_HOUR <= 0:
        _TS_MIN.clear()
        _TS_HOUR.clear()
        return

    while True:
        now = time.time()
        if MAX_PER_MIN > 0:
            cutoff_min = now - 60
            while _TS_MIN and _TS_MIN[0] <= cutoff_min:
                _TS_MIN.popleft()
        else:
            _TS_MIN.clear()

        if MAX_PER_HOUR > 0:
            cutoff_hour = now - 3600
            while _TS_HOUR and _TS_HOUR[0] <= cutoff_hour:
                _TS_HOUR.popleft()
        else:
            _TS_HOUR.clear()

        exceeded_min = MAX_PER_MIN > 0 and len(_TS_MIN) >= MAX_PER_MIN
        exceeded_hour = MAX_PER_HOUR > 0 and len(_TS_HOUR) >= MAX_PER_HOUR
        if not exceeded_min and not exceeded_hour:
            break

        waits: List[float] = []
        if exceeded_min and _TS_MIN:
            waits.append(max(0.0, 60 - (now - _TS_MIN[0])))
        if exceeded_hour and _TS_HOUR:
            waits.append(max(0.0, 3600 - (now - _TS_HOUR[0])))

        wait_for = max(0.1, min(waits) if waits else 0.1)
        time.sleep(wait_for)

    stamp = time.time()
    if MAX_PER_MIN > 0:
        _TS_MIN.append(stamp)
    if MAX_PER_HOUR > 0:
        _TS_HOUR.append(stamp)


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
            self.timeout = int(os.getenv("SMTP_TIMEOUT", "20"))
        except Exception:
            self.timeout = 20
        self.user = os.getenv("EMAIL_ADDRESS")
        self.pwd = os.getenv("EMAIL_PASSWORD")
        self._smtp: Optional[smtplib.SMTP] = None
        self._logged_config = False
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.check_hostname = True
        self._ssl_ctx.verify_mode = ssl.CERT_REQUIRED

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
                self.host,
                self.port,
                timeout=self.timeout,
                context=self._ssl_ctx,
            )
            smtp.ehlo()
        else:
            smtp = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
            smtp.ehlo()
            try:
                smtp.starttls(context=self._ssl_ctx)
            except smtplib.SMTPNotSupportedError:
                # сервер не поддерживает TLS — продолжаем без падения
                pass
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
    for attempt in range(1, retries + 1):
        try:
            _throttle_block()
            return smtp.send(msg)
        except smtplib.SMTPResponseException as e:
            code = getattr(e, "smtp_code", 0)
            if 400 <= code < 500:
                if attempt == retries:
                    raise
                time.sleep(backoff * attempt)
                continue
            if code >= 500:
                raise
            raise
        except (
            smtplib.SMTPServerDisconnected,
            smtplib.SMTPConnectError,
            TimeoutError,
        ):
            if attempt == retries:
                raise
            time.sleep(backoff)
            backoff *= 2
            smtp.ensure()


__all__ = ["RobustSMTP", "send_with_retry"]
