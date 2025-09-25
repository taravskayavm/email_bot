"""Messaging helpers for the aiogram entrypoint."""

from __future__ import annotations

import asyncio
import atexit
import uuid
from typing import Dict, Optional, Tuple

from emailbot.aiogram_port import cooldown
from emailbot.aiogram_port.logs import mask_email
from emailbot.aiogram_port.smtp_sender import SmtpSender
from utils import send_stats

_SENDER: Optional[SmtpSender] = None


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
    send_stats.log_success(to_addr, source, {"trace_id": trace_id, "subject": subject})
    return True, {"trace_id": trace_id, "masked_to": masked}
