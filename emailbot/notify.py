# -*- coding: utf-8 -*-
"""Utilities for Telegram notification filtering."""
from __future__ import annotations

import os
from typing import Any, Literal

MAX_TG = 4096
_PARAGRAPH_CHUNK = 3000


def _split_for_telegram(text: str) -> list[str]:
    parts: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        if not block:
            candidate = current + ("\n\n" if current else "")
            if len(candidate) <= MAX_TG:
                current = candidate
            else:
                if current:
                    parts.append(current)
                current = ""
            continue
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= MAX_TG:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        if len(block) <= MAX_TG:
            current = block
            continue
        start = 0
        while start < len(block):
            chunk = block[start : start + _PARAGRAPH_CHUNK]
            parts.append(chunk)
            start += _PARAGRAPH_CHUNK
    if current:
        parts.append(current)
    return [part for part in parts if part]


async def _safe_reply_text(message, text: str, **kwargs):
    if not text:
        return
    if len(text) <= MAX_TG:
        await message.reply_text(text, **kwargs)
        return
    chunks = _split_for_telegram(text)
    if not chunks:
        return
    first, *rest = chunks
    await message.reply_text(first, **kwargs)
    for part in rest:
        await message.reply_text(part)

# Политика уведомлений:
#   full  — как раньше (все сообщения)
#   brief — только старт/финиш, критические ошибки
#   none  — ничего
NOTIFY_LEVEL = os.getenv("NOTIFY_LEVEL", "full").strip().lower()

Event = Literal[
    "analysis",  # «файл загружен», «анализируем», «подозрительные», и т.д.
    "template_selected",
    "start",  # запуск рассылки
    "progress",  # промежуточные статусы
    "finish",  # финиш рассылки
    "report",  # ежедн./еженед. отчёты
    "error",  # критическая ошибка
]


def _allowed(event: Event) -> bool:
    level = NOTIFY_LEVEL
    if level == "none":
        return event == "error"  # по желанию можно и ошибки заглушить
    if level == "brief":
        return event in {"start", "finish", "error"}
    return True


async def notify(
    message,
    text: str,
    *,
    event: Event = "analysis",
    force: bool = False,
    **kwargs: Any,
) -> None:
    """Send Telegram messages respecting notification level policy."""
    if not text:
        return
    if not force and not _allowed(event):
        return
    await _safe_reply_text(message, text, **kwargs)
