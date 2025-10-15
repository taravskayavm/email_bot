"""Utilities for composing user-facing reports."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, TYPE_CHECKING

from emailbot.suppress_list import is_blocked
from emailbot.utils.fs import append_jsonl_atomic

if TYPE_CHECKING:  # pragma: no cover - typing hints only
    from emailbot.report_preview import PreviewData


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

    needs_ocr = stats.get("needs_ocr")
    if needs_ocr:
        lines.append("💡 Включите OCR для лучшего извлечения")

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
        f"📦 К отправке обработано (с учётом правила «180 дней»): {total}",
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


def _stats_path(path_override: str | None = None) -> Path:
    raw = path_override or os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
    expanded = os.path.expanduser(os.path.expandvars(str(raw)))
    path = Path(expanded)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _preview_group_value(data: "PreviewData") -> str:
    group_code = getattr(data, "group_code", "") or ""
    group_label = getattr(data, "group", "") or ""
    candidate = group_code.strip() or group_label.strip()
    return candidate


def _collect_preview_rows(data: "PreviewData") -> tuple[str, str, list[dict[str, str]]]:
    run_id = (getattr(data, "run_id", "") or "").strip()
    if not run_id:
        return "", "", []
    group_value = _preview_group_value(data)
    sections = [
        list(getattr(data, "valid", []) or []),
        list(getattr(data, "rejected_180d", []) or []),
        list(getattr(data, "blocked", []) or []),
        list(getattr(data, "foreign", []) or []),
        list(getattr(data, "suspicious", []) or []),
        list(getattr(data, "duplicates", []) or []),
    ]
    rows: list[dict[str, str]] = []
    for section in sections:
        for row in section:
            if not isinstance(row, dict):
                continue
            email = str(row.get("email") or "").strip()
            if not email:
                continue
            reason = str(row.get("reason") or "").strip()
            if not reason:
                continue
            source_value = row.get("source") or row.get("source_files") or ""
            source = str(source_value).strip()
            rows.append({"email": email, "reason": reason, "source": source})
    return run_id, group_value, rows


def write_preview_stats(data: "PreviewData", *, stats_path: str | None = None) -> None:
    """Append preview classification rows to ``SEND_STATS_PATH``."""

    if data is None:
        return
    run_id, group_value, rows = _collect_preview_rows(data)
    if not run_id or not rows:
        return
    path = _stats_path(stats_path)
    for row in rows:
        payload = {
            "ts": _now_ts(),
            "email": row["email"],
            "reason": row["reason"],
            "source": row["source"],
            "group": group_value,
            "run_id": run_id,
        }
        append_jsonl_atomic(path, payload)
