"""Utilities for composing user-facing reports."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable, List, Optional


def _now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


_DIGEST_LOGGER = logging.getLogger("emailbot.digest")


def log_extract_digest(stats: dict) -> None:
    """Log a one-line JSON digest for extraction statistics."""

    data = {"ts": _now_ts(), "level": "INFO", "component": "extract"}
    data.update(stats)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def log_mass_filter_digest(ctx: dict) -> None:
    """Log a one-line JSON digest for mass-mail filter statistics."""

    data = {"ts": _now_ts(), "level": "INFO", "component": "mass_filter"}
    data.update(ctx)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def build_mass_report_text(
    sent_ok: Iterable[str],
    skipped_recent: Iterable[str],
    blocked_foreign: Optional[Iterable[str]] = None,
    blocked_invalid: Optional[Iterable[str]] = None,
) -> str:
    """Build summary text for mass mailing.

    Only the ``sent_ok`` and ``skipped_recent`` sections are returned to the user.
    ``blocked_foreign`` and ``blocked_invalid`` are accepted for compatibility but
    ignored in the output so that calling code does not need to change its
    interface.
    """

    def lines(title: str, items: Iterable[str]) -> str:
        items_list = list(items)
        if not items_list:
            return f"{title}: 0\n"
        unique_sorted = sorted(set(items_list))
        return (
            f"{title}: {len(items_list)}\n" +
            "\n".join(f"• {e}" for e in unique_sorted) +
            "\n"
        )

    text: List[str] = []
    text.append(lines("✅ Отправлено", sent_ok))
    text.append(lines("⏳ Пропущены (<180 дней)", skipped_recent))
    return "\n".join(text).strip()
