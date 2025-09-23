import copy
import logging
import os
import smtplib
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable
from email.message import EmailMessage
from email.utils import getaddresses
from smtplib import SMTPResponseException

from emailbot.audit import write_audit_drop
from emailbot.services.cooldown import COOLDOWN_DAYS, should_skip_by_cooldown
from emailbot.history_service import (
    cancel_send_attempt,
    mark_sent,
    register_send_attempt,
)
from utils.send_stats import log_error, log_success


def _extract_group(msg: EmailMessage) -> str:
    raw = msg.get("X-EBOT-Group-Key", "") or msg.get("X-EBOT-Group", "") or ""
    return str(raw).strip()

PORT = int(os.getenv("SMTP_PORT", "587"))
USE_SSL = os.getenv("SMTP_SSL", "0") == "1"
logger = logging.getLogger(__name__)
try:
    TIMEOUT = float(os.getenv("SMTP_TIMEOUT", "30"))
except Exception:
    TIMEOUT = 30.0

DOMAIN_RATE_LIMIT = int(os.getenv("DOMAIN_RATE_LIMIT_PER_MIN", "30"))


class DomainRateLimiter:
    """Ограничитель отправок по домену в расчёте на минуту."""

    def __init__(self, limit_per_min: int) -> None:
        self.limit = max(0, int(limit_per_min))
        self._counters: dict[str, int] = defaultdict(int)
        self._window = None  # type: datetime | None

    def plan(
        self, addresses: list[str]
    ) -> tuple[list[str], list[str], dict[str, int]]:
        if self.limit <= 0:
            return list(addresses), [], {}
        now = datetime.utcnow().replace(second=0, microsecond=0)
        if self._window is None or now != self._window:
            self._counters.clear()
            self._window = now
        send_now: list[str] = []
        defer: list[str] = []
        increments: dict[str, int] = {}
        for addr in addresses:
            domain = _domain_of(addr)
            used = self._counters[domain] + increments.get(domain, 0)
            if used < self.limit:
                increments[domain] = increments.get(domain, 0) + 1
                send_now.append(addr)
            else:
                defer.append(addr)
        return send_now, defer, increments

    def commit(self, increments: dict[str, int]) -> None:
        for domain, count in increments.items():
            if count:
                self._counters[domain] += count


_rate_limiter = DomainRateLimiter(DOMAIN_RATE_LIMIT)


def _domain_of(addr: str) -> str:
    return (addr or "").split("@")[-1].lower().strip()


def _rate_plan(addresses: list[str]) -> tuple[list[str], list[str], dict[str, int]]:
    """Back-compat обёртка для планирования по доменам."""

    return _rate_limiter.plan(addresses)


def _commit_rate_plan(increments: dict[str, int]) -> None:
    """Back-compat обёртка фиксации квоты."""

    _rate_limiter.commit(increments)


def send_messages(messages: Iterable[EmailMessage], user: str, password: str, host: str) -> None:
    """Send multiple e-mails over a single SMTP connection.

    Any failure resets the session so that the next message does not get
    a mysterious ``503 sender already given`` error.
    """
    def _send_one(msg):
        override_flag = str(msg.get("X-EBOT-Override-180d", "") or "").strip().lower()
        to_values = msg.get_all("To", [])
        recipients = [addr for _, addr in getaddresses(to_values)]
        if not recipients:
            raw_to = msg.get("To", "")
            if isinstance(raw_to, str) and raw_to:
                recipients = [raw_to]
        if not recipients and msg.get("X-EBOT-Recipient"):
            recipients = [msg.get("X-EBOT-Recipient")]
        recipients = [addr for addr in recipients if addr]

        send_now, deferred, rate_increments = _rate_plan(recipients)
        if deferred:
            msg["X-EBOT-Deferred"] = ",".join(deferred)
        elif "X-EBOT-Deferred" in msg:
            del msg["X-EBOT-Deferred"]
        if recipients and not send_now:
            return False, SMTPResponseException(451, b"Domain rate limit reached")

        group_key = _extract_group(msg)
        run_id = msg.get("X-EBOT-Run-ID")
        if not run_id:
            run_id = str(uuid.uuid4())
            msg["X-EBOT-Run-ID"] = run_id

        reservation_map: dict[str, datetime] = {}

        def _release_reservations() -> None:
            for addr_norm, reserved_at in list(reservation_map.items()):
                try:
                    cancel_send_attempt(addr_norm, group_key, reserved_at)
                except Exception:  # pragma: no cover - defensive logging
                    logger.debug("history_cancel_failed", exc_info=True)

        if not send_now:
            return True, None

        if override_flag not in {"1", "true", "yes", "on"}:
            for addr in send_now:
                if not addr:
                    continue
                skip, skip_reason = should_skip_by_cooldown(addr)
                if skip:
                    reason_code = (
                        skip_reason.split(";", 1)[0]
                        if skip_reason
                        else f"cooldown<{COOLDOWN_DAYS}d"
                    )
                    try:
                        write_audit_drop(addr, reason_code, skip_reason)
                    except Exception:  # pragma: no cover - defensive logging
                        logger.debug("write_audit_drop failed", exc_info=True)
                    logger.info(
                        "Skipping SMTP send to %s due to cooldown: %s",
                        addr,
                        skip_reason,
                    )
                    return True, None

            if COOLDOWN_DAYS > 0:
                seen_for_reserve: set[str] = set()
                for addr in send_now:
                    addr_norm = (addr or "").strip()
                    if not addr_norm:
                        continue
                    key = addr_norm.lower()
                    if key in seen_for_reserve:
                        continue
                    seen_for_reserve.add(key)
                    reserved_at = register_send_attempt(
                        addr_norm,
                        group_key,
                        days=COOLDOWN_DAYS,
                        run_id=run_id,
                    )
                    if reserved_at is None:
                        reason = f"cooldown<{COOLDOWN_DAYS}d"
                        details = "db-hit"
                        try:
                            write_audit_drop(addr_norm, reason, details)
                        except Exception:  # pragma: no cover - defensive logging
                            logger.debug("write_audit_drop failed", exc_info=True)
                        logger.info(
                            "Skipping SMTP send to %s due to DB cooldown", addr_norm
                        )
                        _release_reservations()
                        return True, None
                    reservation_map[key] = reserved_at

        def _record_success(to_list: list[str], reservations: dict[str, datetime]) -> None:
            extra = {
                "uuid": msg.get("X-EBOT-UUID", ""),
                "message_id": msg.get("Message-ID", ""),
            }
            if run_id:
                extra["run_id"] = run_id
            try:
                to_repr = ", ".join(to_list) if to_list else msg.get("To", "")
                log_success(to_repr, group_key, extra=extra)
            except Exception:
                pass
            try:
                message_id = (msg.get("Message-ID") or "").strip() or None
                seen: set[str] = set()
                for addr in to_list:
                    addr_norm = (addr or "").strip()
                    if not addr_norm:
                        continue
                    key = addr_norm.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    sent_at = reservations.get(key)
                    mark_sent(
                        addr_norm,
                        group_key,
                        message_id,
                        sent_at,
                        run_id=run_id,
                        smtp_result="ok",
                    )
            except Exception:
                logger.warning("history_registry_record_failed", exc_info=True)

        from_addr = os.getenv("EMAIL_ADDRESS", None)
        target_repr = ", ".join(send_now) if send_now else msg.get("To", "")

        def _deliver_all() -> None:
            for addr in send_now:
                if not addr:
                    continue
                one = copy.deepcopy(msg)
                if one.get_all("To"):
                    try:
                        one.replace_header("To", addr)
                    except KeyError:
                        one["To"] = addr
                else:
                    one["To"] = addr
                smtp.send_message(one, from_addr=from_addr, to_addrs=[addr])

        try:
            _deliver_all()
            _commit_rate_plan(rate_increments)
            _record_success(send_now, reservation_map)
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
                    _deliver_all()
                    _commit_rate_plan(rate_increments)
                    _record_success(send_now, reservation_map)
                    return True, None
                except Exception as e2:
                    e = e2
            _release_reservations()
            try:
                log_error(
                    target_repr,
                    group_key,
                    f"{code} {text}",
                    extra={
                        "uuid": msg.get("X-EBOT-UUID", ""),
                        "message_id": msg.get("Message-ID", ""),
                        "run_id": run_id,
                    },
                )
            except Exception:
                pass
            return False, e
        except Exception as e:
            _release_reservations()
            try:
                log_error(
                    target_repr,
                    group_key,
                    repr(e),
                    extra={
                        "uuid": msg.get("X-EBOT-UUID", ""),
                        "message_id": msg.get("Message-ID", ""),
                        "run_id": run_id,
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
