"""Inline keyboards used by the aiogram-based bot."""

from __future__ import annotations

import json
import os
import unicodedata
from pathlib import Path

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

_ICONS_ENV = os.getenv("DIRECTION_ICONS_PATH")
if _ICONS_ENV:
    ICONS_PATH = Path(os.path.expandvars(os.path.expanduser(_ICONS_ENV))).resolve()
else:
    ICONS_PATH = Path(__file__).resolve().parents[2] / "icons.json"
_DEFAULT_ICON = "📄"


def _norm(value: str) -> str:
    """Normalize keys for consistent lookup."""

    return unicodedata.normalize("NFKC", (value or "")).strip().lower()


def _normalize_mapping(mapping: object) -> dict[str, str]:
    if not isinstance(mapping, dict):
        return {}

    result: dict[str, str] = {}
    for key, value in mapping.items():
        normalized_key = _norm(str(key))
        if not normalized_key:
            continue
        result[normalized_key] = str(value)
    return result


def _load_icons() -> dict[str, str]:
    """Load icons mapping with support for env overrides and normalization."""

    raw = os.getenv("DIRECTION_ICONS_JSON")
    if raw:
        try:
            return _normalize_mapping(json.loads(raw))
        except Exception:
            pass

    if ICONS_PATH.exists():
        try:
            return _normalize_mapping(json.loads(ICONS_PATH.read_text(encoding="utf-8")))
        except Exception:
            return {}
    return {}


def _label_with_icon(direction: str, icons_norm: dict[str, str]) -> str:
    icon = icons_norm.get(_norm(direction)) or _DEFAULT_ICON
    icon = unicodedata.normalize("NFKC", icon)
    return f"{icon} {direction}".strip()


def directions_keyboard(directions: list[str]) -> InlineKeyboardMarkup:
    """Build direction selection keyboard with icons from icons.json."""

    icons = _load_icons()
    builder = InlineKeyboardBuilder()
    for direction in directions:
        builder.button(
            text=_label_with_icon(direction, icons),
            callback_data=f"set_group:{direction}",
        )
    builder.adjust(1)
    return builder.as_markup()


def send_flow_keyboard() -> InlineKeyboardMarkup:
    """Keyboard shown before sending bulk e-mails."""

    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Отправить", callback_data="bulk:send:start")
    builder.button(text="↩️ Вернуться / Править", callback_data="bulk:send:back")
    builder.button(text="✏️ Исправить адрес", callback_data="bulk:send:edit")
    builder.adjust(1)
    return builder.as_markup()
