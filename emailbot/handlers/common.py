"""Common helpers for Telegram handlers."""

from __future__ import annotations

from typing import Optional

from telegram.error import BadRequest


async def safe_answer(
    query,
    text: Optional[str] = None,
    show_alert: bool = False,
    cache_time: int = 0,
) -> None:
    """Safely answer a callback query ignoring transient errors.

    Telegram raises :class:`BadRequest` for stale queries ("Query is too old"),
    among other minor issues. They should not crash the handler, so we swallow
    them quietly. Passing ``None`` for ``query`` is also tolerated.
    """

    if query is None:
        return
    try:
        await query.answer(text=text or "", show_alert=show_alert, cache_time=cache_time)
    except BadRequest:
        # "Query is too old" and similar edge cases – ignore.
        return
