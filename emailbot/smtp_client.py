"""SMTP helpers used across the project.

This module hosts the canonical implementations of both the simple context
manager based client as well as the higher level retrying helper that used to
live under :mod:`utils.smtp_client`.
"""
from __future__ import annotations

import logging
import os
import random
import smtplib
import ssl
import time
from collections import defaultdict, deque
from email.message import EmailMessage
from email.utils import getaddresses
from typing import Deque, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


def _env_float(env_name: str, default: float) -> float:
    try:
        return float(os.getenv(env_name, str(default)))
    except Exception:
        return default


BASE_DOMAIN_DELAY = _env_float("SMTP_BASE_DOMAIN_DELAY", 0.0)
BACKOFF_STEP_SECONDS = _env_float("SMTP_BACKOFF_STEP_SECONDS", 1.0)
BACKOFF_MAX_SECONDS = _env_float("SMTP_BACKOFF_MAX_SECONDS", 30.0)
BACKOFF_DECAY_SUCCESS = _env_float("SMTP_BACKOFF_DECAY_SUCCESS", 1.0)

# Дополнительный мягкий «джиттер» между отправками, чтобы не раздражать антиспам-эвристики.
# По умолчанию 0 — поведение не меняется. Значения в миллисекундах.
_JITTER_MIN_MS = _env_float("SMTP_JITTER_MIN_MS", 0.0)
_JITTER_MAX_MS = _env_float("SMTP_JITTER_MAX_MS", 0.0)


def _maybe_jitter_sleep() -> None:
    try:
        jmin = max(0.0, _JITTER_MIN_MS)
        jmax = max(0.0, _JITTER_MAX_MS)
        if jmax <= 0.0 and jmin <= 0.0:
            return
        if jmax < jmin:
            jmax = jmin
        delay = random.uniform(jmin, jmax) / 1000.0
        if delay > 0:
            time.sleep(delay)
    except Exception:
        # джиттер не должен ломать отправку
        pass


class SmtpClient:
    """Simple SMTP client with context manager support."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        use_ssl: Optional[bool] = None,
        timeout: Optional[float] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        # Если SMTP_SSL не задана, автоматически включаем SSL на 465 порту.
        if use_ssl is None:
            env = os.getenv("SMTP_SSL")
            if env is None or env == "":
                use_ssl = self.port == 465
            else:
                use_ssl = env == "1"
        self.use_ssl = use_ssl
        if timeout is None:
            try:
                timeout = float(os.getenv("SMTP_TIMEOUT", "30"))
            except Exception:
                timeout = 30.0
        self.timeout = timeout
        self._server: Optional[smtplib.SMTP] = None

    def __enter__(self) -> "SmtpClient":
        context = ssl.create_default_context()
        if self.use_ssl:
            self._server = smtplib.SMTP_SSL(
                self.host, self.port, timeout=self.timeout, context=context
            )
        else:
            self._server = smtplib.SMTP(
                self.host, self.port, timeout=self.timeout
            )
            self._server.starttls(context=context)
        self._server.login(self.username, self.password)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            try:
                self._server.quit()
            finally:
                self._server = None

    def send(self, sender: str, recipient: str, raw_message: str) -> None:
        if self._server is None:
            raise RuntimeError("SMTP client is not connected")
        self._server.sendmail(sender, recipient, raw_message)


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
_DOMAIN_DELAYS: defaultdict[str, float] = defaultdict(lambda: BASE_DOMAIN_DELAY)


def _extract_domains(msg: EmailMessage) -> Set[str]:
    headers: list[str] = []
    for name in ("To", "Cc", "Bcc"):
        headers.extend(msg.get_all(name, []))
    domains: Set[str] = set()
    for _, addr in getaddresses(headers):
        if not addr or "@" not in addr:
            continue
        domain = addr.split("@", 1)[1].strip().lower()
        if domain:
            domains.add(domain)
    return domains


def _sleep_for_domains(domains: Iterable[str]) -> None:
    delays = [max(0.0, _DOMAIN_DELAYS[d]) for d in set(domains) if d]
    if not delays:
        return
    wait_for = max(delays)
    if wait_for > 0:
        time.sleep(wait_for)


def _backoff_fail(domains: Iterable[str]) -> None:
    for domain in set(domains):
        if not domain:
            continue
        current = _DOMAIN_DELAYS[domain]
        _DOMAIN_DELAYS[domain] = min(
            BACKOFF_MAX_SECONDS, current + BACKOFF_STEP_SECONDS
        )


def _backoff_success(domains: Iterable[str]) -> None:
    for domain in set(domains):
        if not domain:
            continue
        current = _DOMAIN_DELAYS[domain]
        _DOMAIN_DELAYS[domain] = max(
            BASE_DOMAIN_DELAY, current - BACKOFF_DECAY_SUCCESS
        )


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
    smtp: RobustSMTP,
    msg: EmailMessage,
    *,
    retries: int = 2,
    backoff: float = 1.0,
):
    domains = _extract_domains(msg)
    for attempt in range(1, retries + 1):
        try:
            _sleep_for_domains(domains)
            _throttle_block()
            # Перед фактической отправкой делаем мягкую случайную паузу (если включена).
            _maybe_jitter_sleep()
            result = smtp.send(msg)
            _backoff_success(domains)
            return result
        except smtplib.SMTPResponseException as e:
            code = getattr(e, "smtp_code", 0)
            if 400 <= code < 500:
                _backoff_fail(domains)
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
            _backoff_fail(domains)
            if attempt == retries:
                raise
            time.sleep(backoff)
            backoff *= 2
            smtp.ensure()


__all__ = ["SmtpClient", "RobustSMTP", "send_with_retry"]
