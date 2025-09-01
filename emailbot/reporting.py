"""Utilities for composing user-facing reports."""

from __future__ import annotations

from typing import Iterable, List, Optional


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
