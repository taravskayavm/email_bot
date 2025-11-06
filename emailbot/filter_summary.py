from __future__ import annotations

from typing import Mapping


def render_filter_summary(stats: Mapping[str, object] | None) -> str:
    """Return a concise summary of filtered address counters."""

    if not stats:
        return ""

    labels = {
        "role": "роль-адресов",
        "stoplist": "стоп-лист",
        "cooldown": "кулдаун",
        "duplicates": "дубликаты",
        "invalid": "некорректные",
    }

    parts: list[str] = []
    for key, label in labels.items():
        try:
            raw = stats.get(key, 0)  # type: ignore[assignment]
        except AttributeError:
            return ""
        try:
            count = int(raw)
        except (TypeError, ValueError):
            continue
        if count:
            parts.append(f"{label} {count}")

    if not parts:
        return ""

    return "Отфильтровано: " + ", ".join(parts)

