"""Helper services for the email bot package."""

from .cooldown import (
    COOLDOWN_DAYS,
    APPEND_TO_SENT,
    COOLDOWN_WINDOW_DAYS,
    check_email,
    get_last_sent_at,
    mark_sent,
    normalize_email_for_key,
    should_skip_by_cooldown,
    was_sent_recently,
)

__all__ = [
    "COOLDOWN_DAYS",
    "APPEND_TO_SENT",
    "COOLDOWN_WINDOW_DAYS",
    "check_email",
    "normalize_email_for_key",
    "get_last_sent_at",
    "should_skip_by_cooldown",
    "was_sent_recently",
    "mark_sent",
]
