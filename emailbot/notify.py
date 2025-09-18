# -*- coding: utf-8 -*-
"""Utilities for Telegram notification filtering."""
from __future__ import annotations

import os
from typing import Any, Literal

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
    await message.reply_text(text, **kwargs)
