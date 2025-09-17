from __future__ import annotations

from collections import OrderedDict
from typing import Dict

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from services.templates import list_templates


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
    for key, info in deduped.items():
        label = info.get("label") or info.get("code") or key
        if normalized_current and key == normalized_current:
            label = f"{label} • текущий"
        rows.append(
            [InlineKeyboardButton(str(label), callback_data=f"{prefix}{key}")]
        )
        mapping[key] = dict(info)

    if context is not None:
        storage = context.user_data.setdefault(map_name, {})
        storage[prefix] = mapping

    return InlineKeyboardMarkup(rows)
