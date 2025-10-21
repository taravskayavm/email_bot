"""Manual input handler helpers for the Telegram bot."""

from __future__ import annotations

import re
from typing import Iterable

from aiogram import F, Router, types

from emailbot.reporting import count_blocked
from emailbot.ui.messages import render_dispatch_summary
from emailbot.utils.email_clean import preclean_for_email_extraction
from utils.email_clean import parse_emails_unified


router = Router()

_URL_RE = re.compile(r"^(https?://|www\.)[^\s]+$", re.IGNORECASE)


def _looks_like_url(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    return bool(_URL_RE.match(text))


def _unique(sequence: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in sequence:
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(item)
    return result


def _extract_manual_emails(raw: str) -> list[str]:
    cleaned = preclean_for_email_extraction(raw or "")
    emails, _meta = parse_emails_unified(cleaned, return_meta=True)
    return _unique(emails)


def _render_summary(emails: list[str]) -> str:
    blocked = 0
    try:
        blocked = count_blocked(emails)
    except Exception:
        blocked = 0
    return render_dispatch_summary(
        planned=len(emails),
        sent=0,
        skipped_cooldown=0,
        skipped_initial=0,
        errors=0,
        audit_path=None,
        planned_emails=emails,
        raw_emails=emails,
        blocked_count=blocked,
    )


@router.message(F.text)
async def handle_manual_input(message: types.Message) -> None:
    """Handle a manual user input string with addresses or URLs."""

    text = (message.text or "").strip()
    if not text:
        await message.answer("Пришлите адреса или ссылку ещё раз.")
        return

    if "@" in text:
        emails = _extract_manual_emails(text)
        if emails:
            await message.answer(_render_summary(emails))
            return
        await message.answer(
            "Не удалось распознать адреса. Пришлите e-mail'ы через пробел, запятую или с новой строки."
        )
        return

    if _looks_like_url(text):
        await message.answer(
            "🔒 В ручном режиме ссылки не принимаются.\n"
            "Отправьте только e-mail-адреса, либо используйте режим массовой рассылки для парсинга сайтов."
        )
        return

    emails = _extract_manual_emails(text)
    if emails:
        await message.answer(_render_summary(emails))
        return
    await message.answer("Не распознал ни ссылку, ни e-mail-адреса. Пришлите ещё раз.")


def parse_manual_input(text: str) -> list[str]:
    """Compatibility helper used by legacy tests."""

    return _extract_manual_emails(text)


__all__ = ["router", "handle_manual_input", "parse_manual_input"]

