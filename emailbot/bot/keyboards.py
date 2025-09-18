"""Inline keyboards used by the aiogram-based bot."""

from __future__ import annotations

from pathlib import Path
import json

from typing import Dict

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

ICONS_PATH = Path("icons.json")


def _load_icons() -> dict[str, str]:
    if ICONS_PATH.exists():
        try:
            return json.loads(ICONS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _icon_for(label: str, icons: Dict[str, str]) -> str:
    if not label:
        return ""
    key = label.strip()
    if not key:
        return ""
    icon = icons.get(key)
    if icon:
        return icon
    capitalized = icons.get(key.capitalize())
    if capitalized:
        return capitalized
    lowered = key.casefold()
    for stored_key, stored_icon in icons.items():
        if stored_key.strip().casefold() == lowered:
            return stored_icon
    return ""


def _label_with_icon(label: str, icons: dict[str, str]) -> str:
    icon = _icon_for(label, icons)
    return f"{icon} {label}" if icon else label


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
