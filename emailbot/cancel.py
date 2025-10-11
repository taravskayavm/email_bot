"""Simple per-chat cancellation tokens for long-running tasks."""

from __future__ import annotations

import threading
from typing import Dict

_LIVE_TOKENS: Dict[int, threading.Event] = {}


def start_cancel(chat_id: int) -> None:
    """Create or reset a cancellation token for ``chat_id``."""

    event = _LIVE_TOKENS.get(chat_id)
    if event is None:
        event = threading.Event()
        _LIVE_TOKENS[chat_id] = event
    else:
        event.clear()


def request_cancel(chat_id: int) -> None:
    """Set cancellation flag for ``chat_id`` if it exists."""

    event = _LIVE_TOKENS.get(chat_id)
    if event is not None:
        event.set()


def is_cancelled(chat_id: int) -> bool:
    """Return ``True`` if cancellation was requested for ``chat_id``."""

    event = _LIVE_TOKENS.get(chat_id)
    return bool(event and event.is_set())


def clear_cancel(chat_id: int) -> None:
    """Clear the cancellation flag for ``chat_id`` if present."""

    event = _LIVE_TOKENS.get(chat_id)
    if event is not None:
        event.clear()
