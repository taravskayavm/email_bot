"""User interface helpers for inline keyboards (Telegram)."""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

_DEFAULT_ICONS = {
    "bioinformatics": "🧬",
    "geography": "🗺️",
    "psychology": "🧠",
    "beauty": "💅",
    "medicine": "💊",
    "sport": "🏃",
    "tourism": "🌍",
}


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _extract_code_label(item: object) -> tuple[str, str]:
    """Return ``(code, label)`` from supported direction descriptors."""

    if isinstance(item, Mapping):
        code = str(item.get("code") or item.get("value") or item.get("slug") or "").strip()
        label = str(
            item.get("label")
            or item.get("title")
            or item.get("name")
            or code
        ).strip()
        return code, label or code
    if isinstance(item, (tuple, list)) and item:
        first = str(item[0]).strip()
        second = str(item[1] if len(item) > 1 else item[0]).strip()
        return first, second or first
    if isinstance(item, str):
        value = item.strip()
        return value, value
    return "", ""


def directions_keyboard(
    directions: Mapping[str, str]
    | Sequence[tuple[str, str]]
    | Sequence[Mapping[str, str]]
    | Sequence[str],
    *,
    selected_code: str | None = None,
    prefix: str = "group_",
    icons: Mapping[str, str] | None = None,
) -> InlineKeyboardMarkup:
    """Build an inline keyboard for selecting mailing directions."""

    mapping: dict[str, str] = {}
    if isinstance(directions, Mapping):
        mapping = {str(code).strip(): str(label).strip() for code, label in directions.items()}
        items: Iterable[tuple[str, str]] = mapping.items()
    else:
        items = []
        normalized: list[tuple[str, str]] = []
        for entry in directions:
            code, label = _extract_code_label(entry)
            code_norm = code.strip()
            if not code_norm:
                continue
            normalized.append((code_norm, label or code_norm))
        items = normalized

    icons_map = {**_DEFAULT_ICONS, **(icons or {})}
    selected_norm = _normalize(selected_code)
    rows: list[list[InlineKeyboardButton]] = []
    for code, label in items:
        code = str(code).strip()
        if not code:
            continue
        label = (label or code).strip()
        icon = icons_map.get(_normalize(code), "").strip()
        text = f"{icon} {label}".strip()
        if selected_norm and _normalize(code) == selected_norm:
            text = f"{text} ✅"
        rows.append([InlineKeyboardButton(text, callback_data=f"{prefix}{code}")])
    return InlineKeyboardMarkup(rows)
