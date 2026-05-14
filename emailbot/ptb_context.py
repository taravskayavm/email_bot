"""Helpers for sharing Telegram runtime context across modules."""

from __future__ import annotations

from typing import Optional

from telegram import Chat
from telegram.ext import Application

__all__ = [
    "set_application",
    "get_application",
    "set_current_chat",
    "get_current_chat_id",
]

_application: Application | None = None
_current_chat_id: Optional[int] = None


def set_application(app: Application | None) -> None:
    """Store the active PTB ``Application`` for background callbacks."""

    global _application
    _application = app


def get_application() -> Application | None:
    """Return the stored PTB ``Application`` if available."""

    return _application


def set_current_chat(chat: Chat | None) -> None:
    """Remember the most recent chat identifier for UI notifications."""

    global _current_chat_id
    if chat is None:
        return
    try:
        chat_id = chat.id
    except AttributeError:
        return
    _current_chat_id = int(chat_id)


def get_current_chat_id() -> Optional[int]:
    """Return the identifier of the most recently observed chat."""

    return _current_chat_id
