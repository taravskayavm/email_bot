"""Общие вспомогательные функции для Telegram-хендлеров."""

from __future__ import annotations

from typing import Optional

from telegram.error import BadRequest


async def safe_answer(
    query,
    text: Optional[str] = None,
    show_alert: bool = False,
    cache_time: int = 0,
) -> None:
    """Безопасный ответ для CallbackQuery.

    Бывает, что Telegram возвращает ``BadRequest`` для слишком старых
    запросов («Query is too old») или если ID колбэка уже недействителен.
    Такие ситуации не являются критическими, поэтому мы их молча
    игнорируем, продолжая обработку. Передача ``None`` вместо запроса также
    допустима.
    """

    if not query:
        return
    try:
        await query.answer(text=text or "", show_alert=show_alert, cache_time=cache_time)
    except BadRequest as err:
        message = str(err)
        if "Query is too old" in message or "query id is invalid" in message:
            # протухший или недействительный id — просто игнорируем
            return
        raise
