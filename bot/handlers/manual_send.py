"""Manual input handler helpers for the Telegram bot."""

from __future__ import annotations

from typing import Iterable

from aiogram import F, Router, types

from emailbot.reporting import count_blocked
from emailbot.ui.messages import render_dispatch_summary
from utils.email_clean import (
    contains_url_but_not_email,
    parse_emails_unified,
    preclean_for_email_extraction,
)


router = Router()


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

    cleaned = preclean_for_email_extraction(text)
    emails, _meta = parse_emails_unified(cleaned, return_meta=True)
    if emails:
        await message.answer(_render_summary(_unique(emails)))
        return

    if contains_url_but_not_email(cleaned):
        await message.answer(
            "🔒 В ручном режиме ссылки не принимаются.\n"
            "Отправьте только e-mail-адреса, либо используйте режим массовой рассылки для парсинга сайтов."
        )
        return

    await message.answer(
        "Не нашла корректных адресов. Пришлите ещё раз (допустимы запятая/пробел/новая строка)."
    )


def parse_manual_input(text: str) -> list[str]:
    """Compatibility helper used by legacy tests."""

    return _extract_manual_emails(text)


__all__ = ["router", "handle_manual_input", "parse_manual_input"]

