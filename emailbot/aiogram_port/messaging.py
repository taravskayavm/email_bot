"""Messaging helpers for the aiogram entrypoint."""

from __future__ import annotations

import asyncio
import atexit
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from emailbot.aiogram_port import cooldown
from emailbot.aiogram_port.logs import mask_email
from emailbot.aiogram_port.smtp_sender import SmtpSender
from emailbot.messaging_utils import prepare_recipients_for_send
from emailbot import history_service
from utils import send_stats

_SENDER: Optional[SmtpSender] = None

logger = logging.getLogger(__name__)


def _get_sender() -> SmtpSender:
    global _SENDER
    if _SENDER is None:
        _SENDER = SmtpSender()
    return _SENDER


def _close_sender() -> None:
    global _SENDER
    try:
        if _SENDER is not None:
            _SENDER.close()
    finally:
        _SENDER = None


atexit.register(_close_sender)


def _trace_id() -> str:
    return uuid.uuid4().hex[:8]


async def _send_via_smtp(**kwargs) -> None:
    sender = _get_sender()
    await asyncio.to_thread(sender.send, **kwargs)


async def send_one_email(
    to_addr: str,
    subject: str,
    body: str,
    *,
    source: str,
    html: Optional[str] = None,
) -> Tuple[bool, Dict[str, str]]:
    """Send an e-mail after enforcing cooldown and logging outcomes."""

    trace_id = _trace_id()

    good, dropped, remap = prepare_recipients_for_send([to_addr])
    if not good:
        logger.info(
            "Recipient dropped after preprocess", extra={
                "raw": to_addr,
                "dropped": sorted(dropped),
                "remap": remap,
                "trace_id": trace_id,
            }
        )
        reason = "recipient rejected after sanitisation"
        if dropped:
            reason = f"recipient rejected after sanitisation: {', '.join(sorted(dropped))}"
        return False, {
            "trace_id": trace_id,
            "masked_to": mask_email(to_addr),
            "reason": reason,
        }

    if remap and to_addr != good[0]:
        logger.debug("Recipient remapped before send: %s -> %s", to_addr, good[0])
    to_addr = good[0]

    masked = mask_email(to_addr)
    allowed, reason = cooldown.enforce_cooldown(to_addr)
    if not allowed:
        return False, {
            "trace_id": trace_id,
            "masked_to": masked,
            "reason": reason or "cooldown active",
        }

    try:
        await _send_via_smtp(to_addr=to_addr, subject=subject, body=body, html=html)
    except Exception as exc:  # pragma: no cover - network related
        send_stats.log_error(to_addr, source, str(exc), {"trace_id": trace_id})
        return False, {
            "trace_id": trace_id,
            "masked_to": masked,
            "reason": str(exc),
        }

    cooldown.mark_sent(to_addr)
    try:
        history_service.mark_sent(
            to_addr,
            source or "__aiogram__",
            None,
            datetime.now(timezone.utc),
            smtp_result="ok",
        )
    except Exception:
        logger.debug("history mark_sent failed (non-fatal)", exc_info=True)
    send_stats.log_success(to_addr, source, {"trace_id": trace_id, "subject": subject})
    return True, {"trace_id": trace_id, "masked_to": masked}
