"""UI helpers for sending contextual notifications from background tasks."""

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from emailbot.ptb_context import get_application, get_current_chat_id

__all__ = ["notify_timeout_hint"]


def notify_timeout_hint(filename: str, timeout_used: int | float) -> None:
    """Schedule a hint about enabling the heavy profile after a PDF timeout."""

    app = get_application()
    chat_id = get_current_chat_id()
    if app is None or chat_id is None:
        return

    text = (
        f"⏱️ Файл *{filename}* не успел обработаться за {timeout_used:.1f} с.\n"
        "Совет: включите профиль *Тяжёлый* (больше таймаут + OCR) и повторите."
    )
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🧱 Включить тяжёлый профиль и повторить",
                    callback_data="profile:set:heavy",
                )
            ]
        ]
    )

    async def _send_hint() -> None:
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=markup,
            )
        except Exception:
            # Notification is best-effort; ignore transport errors.
            pass

    try:
        app.create_task(_send_hint())
    except Exception:
        # If the application is shutting down, scheduling may fail. Ignore silently.
        pass
