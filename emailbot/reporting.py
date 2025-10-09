"""Utilities for composing user-facing reports."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable, List, Optional

from emailbot.suppress_list import is_blocked


def _now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


_DIGEST_LOGGER = logging.getLogger("emailbot.digest")


def log_extract_digest(stats: dict) -> None:
    """Log a one-line JSON digest for extraction statistics."""

    data = {
        "ts": _now_ts(),
        "level": "INFO",
        "component": "extract",
        "footnote_singletons_repaired": stats.get("footnote_singletons_repaired", 0),
        "footnote_guard_skips": stats.get("footnote_guard_skips", 0),
        "footnote_ambiguous_kept": stats.get("footnote_ambiguous_kept", 0),
        "left_guard_skips": stats.get("left_guard_skips", 0),
        "prefix_expanded": stats.get("prefix_expanded", 0),
        "phone_prefix_stripped": stats.get("phone_prefix_stripped", 0),
    }
    data.update(stats)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def log_mass_filter_digest(ctx: dict) -> None:
    """Log a one-line JSON digest for mass-mail filter statistics."""

    data = {"ts": _now_ts(), "level": "INFO", "component": "mass_filter"}
    data.update(ctx)
    _DIGEST_LOGGER.info(json.dumps(data, ensure_ascii=False))


def render_summary(stats: dict) -> str:
    """Render a short textual summary for extraction statistics."""

    lines: List[str] = []

    total_found = stats.get("total_found")
    if total_found is not None:
        lines.append(f"📊 Найдено адресов: {total_found}")

    to_send = stats.get("unique_after_cleanup")
    if to_send is None:
        to_send = stats.get("total_ready", 0)
    lines.append(f"📦 К отправке: {to_send}")

    suspicious = stats.get("suspicious_numeric_localpart")
    if suspicious:
        lines.append(f"🟡 Подозрительные: {suspicious}")

    blocked_total = stats.get("blocked_total", 0)
    lines.append(f"🚫 Из блок-листа: {blocked_total}")

    missed_pages = stats.get("pdf_pages_failed")
    if missed_pages:
        lines.append(f"📄 Не распознаны страницы PDF: {missed_pages}")

    invalid_tld = stats.get("invalid_tld")
    if invalid_tld:
        lines.append(f"❗ Некорректные домены: {invalid_tld}")

    return "\n".join(lines)


def build_mass_report_text(
    sent_ok: Iterable[str],
    skipped_recent: Iterable[str],
    blocked_foreign: Optional[Iterable[str]] = None,
    blocked_invalid: Optional[Iterable[str]] = None,
    duplicates_24h: Optional[Iterable[str]] = None,
) -> str:
    """Build summary text for mass mailing.

    The function returns only aggregate counts without revealing individual
    e‑mail addresses. ``blocked_foreign`` and ``blocked_invalid`` are accepted for
    backward compatibility and counted in the summary.
    """

    sent_cnt = len(list(sent_ok))
    skipped_cnt = len(list(skipped_recent))
    blocked_cnt = len(list(blocked_invalid or []))
    foreign_cnt = len(list(blocked_foreign or []))
    dup_cnt = len(list(duplicates_24h or []))
    total = sent_cnt + skipped_cnt + blocked_cnt + foreign_cnt + dup_cnt

    lines = [
        "✉️ Рассылка завершена.",
        f"📦 В очереди было: {total}",
        f"✅ Успешно отправлено: {sent_cnt}",
        f"⏳ Пропущены (по правилу «180 дней»): {skipped_cnt}",
        f"🚫 В блок-листе/недоступны: {blocked_cnt}",
        f"🌍 Иностранные (отложены): {foreign_cnt}",
    ]
    if dup_cnt:
        lines.append(f"🔁 Дубликаты за 24 ч: {dup_cnt}")
    return "\n".join(lines)


def count_blocked(emails: Iterable[str]) -> int:
    """Return how many addresses are present in the block list."""

    return sum(1 for email in emails if is_blocked(email))
