from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from services.templates import list_templates

_ICONS: Dict[str, str] = {}
_ICONS_PATH = Path("icons.json")
if _ICONS_PATH.exists():
    try:
        _ICONS = json.loads(_ICONS_PATH.read_text(encoding="utf-8"))
    except Exception:
        _ICONS = {}


def _icon_for(label: str) -> str:
    if not label:
        return ""
    key = label.strip()
    if not key:
        return ""
    icon = _ICONS.get(key)
    if icon:
        return icon
    capitalized = _ICONS.get(key.capitalize())
    if capitalized:
        return capitalized
    lowered = key.casefold()
    for stored_key, stored_icon in _ICONS.items():
        if stored_key.strip().casefold() == lowered:
            return stored_icon
    return ""


def _normalize_code(code: str | None) -> str:
    return (code or "").strip().casefold()


def build_templates_kb(
    context: ContextTypes.DEFAULT_TYPE,
    current_code: str | None = None,
    prefix: str = "tpl:",
    map_name: str = "groups_map",
) -> InlineKeyboardMarkup:
    """Build inline keyboard with templates deduplicated by code."""

    templates = list_templates()
    deduped: "OrderedDict[str, Dict[str, str]]" = OrderedDict()
    for tpl in templates:
        code = str(tpl.get("code") or "").strip()
        key = _normalize_code(code)
        if not key or key in deduped:
            continue
        label = str(tpl.get("label") or code or key).strip()
        path = str(tpl.get("path") or "")
        info: Dict[str, str] = {
            "code": code,
            "label": label,
            "path": path,
        }
        for extra_key, extra_value in tpl.items():
            if extra_key in info:
                continue
            info[extra_key] = extra_value
        deduped[key] = info

    normalized_current = _normalize_code(current_code)
    rows = []
    mapping: Dict[str, Dict[str, str]] = {}
    label_rows: Dict[str, int] = {}
    for key, info in deduped.items():
        mapping[key] = dict(info)
        base_label = str(info.get("label") or info.get("code") or key)
        display_label = base_label
        # Префиксуем иконкой, если она задана в icons.json
        icon = _icon_for(base_label)
        if icon:
            display_label = f"{icon} {display_label}"
        if normalized_current and key == normalized_current:
            display_label = f"{display_label} • текущий"
        existing_idx = label_rows.get(base_label)
        button = InlineKeyboardButton(
            str(display_label), callback_data=f"{prefix}{key}"
        )
        if existing_idx is None:
            rows.append([button])
            label_rows[base_label] = len(rows) - 1
        else:
            if normalized_current and key == normalized_current:
                rows[existing_idx] = [button]

    if context is not None:
        storage = context.user_data.setdefault(map_name, {})
        storage[prefix] = mapping

    return InlineKeyboardMarkup(rows)
