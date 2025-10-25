from __future__ import annotations

from typing import Iterable

from services.ui_state import foreign_allowed_for_batch
from utils.geo_domains import is_foreign_email


def _auto_foreign_count(found: Iterable[str]) -> int:
    return sum(1 for email in found if is_foreign_email(email))


def build_summary(stats: dict) -> str:
    found = list(stats.get("found", []))
    lines: list[str] = []

    total_found = stats.get("total_found")
    if total_found is None:
        total_found = len(found)
    lines.append(f"🔍 Найдено адресов: {total_found}")

    unique_total = stats.get("unique_total")
    if unique_total is not None:
        lines.append(f"📬 Уникальных: {unique_total}")

    to_send = stats.get("to_send")
    if to_send is None:
        to_send = len(found)
    lines.append(f"📦 К отправке: {to_send}")

    foreign_count = stats.get("foreign_count")
    if foreign_count is None:
        foreign_count = _auto_foreign_count(found)
    lines.append(f"🌍 Иностранные домены: {foreign_count}")

    if "batch_id" in stats and "chat_id" in stats:
        allow = foreign_allowed_for_batch(stats["batch_id"], stats["chat_id"])
        lines.append("🔘 В рассылку иностранные: " + ("✅ да" if allow else "🚫 нет"))

    blocked = stats.get("blocked_total")
    if blocked is not None:
        lines.append(f"🚫 Из стоп-листа: {blocked}")

    return "\n".join(lines)


__all__ = ["build_summary"]
