"""UI helpers for sending contextual notifications from background tasks."""

from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from emailbot.ptb_context import get_application, get_current_chat_id

__all__ = [
    "notify_timeout_hint",
    "remember_timeout_hint_target",
    "forget_timeout_hint_target",
]

_timeout_targets: dict[str, int] = {}
_timeout_lock = Lock()


def _normalize_path(source: str | Path | None) -> str | None:
    if source is None:
        return None
    try:
        return str(Path(source).resolve())
    except Exception:
        return str(source)


def remember_timeout_hint_target(source: str | Path, chat_id: int | str) -> None:
    """Associate ``source`` with ``chat_id`` for future timeout notifications."""

    try:
        chat_int = int(chat_id)
    except (TypeError, ValueError):
        return
    key = _normalize_path(source)
    if not key:
        return
    with _timeout_lock:
        _timeout_targets[key] = chat_int


def forget_timeout_hint_target(source: str | Path | None) -> None:
    """Drop a previously remembered timeout notification target."""

    key = _normalize_path(source)
    if not key:
        return
    with _timeout_lock:
        _timeout_targets.pop(key, None)


def _resolve_timeout_chat(
    *, source: str | Path | None, explicit_chat: Optional[int]
) -> Optional[int]:
    if explicit_chat is not None:
        return explicit_chat
    key = _normalize_path(source)
    if key:
        with _timeout_lock:
            cached = _timeout_targets.pop(key, None)
        if cached is not None:
            return cached
    return get_current_chat_id()


def notify_timeout_hint(
    filename: str,
    timeout_used: int | float,
    *,
    chat_id: int | None = None,
    source_path: str | Path | None = None,
) -> None:
    """Schedule a hint about enabling the heavy profile after a PDF timeout."""

    app = get_application()
    if app is None:
        return
    resolved_chat = _resolve_timeout_chat(source=source_path, explicit_chat=chat_id)
    if resolved_chat is None:
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
                chat_id=resolved_chat,
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
