from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Sequence

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes


groups_map = {
    "sport": "⚽ Спорт",
    "tourism": "🧭 Туризм",
    "medicine": "🩺 Медицина",
    "bioinformatics": "🧬 Биоинформатика",
    "geography": "🗺️ География",
    "psychology": "🧠 Психология",
    "beauty": "💄 Индустрия красоты",
}


def build_parse_mode_kb(
    token: str,
    last_sections: list[str] | None = None,
    domain: str | None = None,
) -> InlineKeyboardMarkup:
    """Keyboard offering parse mode selection for a detected URL token."""

    value = (token or "").strip()
    rows = [
        [
            InlineKeyboardButton(
                "📄 Только эта страница", callback_data=f"parse|single|{value}"
            ),
            InlineKeyboardButton(
                "🕸️ Сканировать сайт", callback_data=f"parse|deep|{value}"
            ),
        ]
    ]
    rows.append(
        [
            InlineKeyboardButton(
                "🕸️ Выбрать разделы…", callback_data=f"parse|sections|{value}"
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                "🔎 Предложить разделы", callback_data=f"parse|suggest|{value}"
            )
        ]
    )
    if last_sections:
        human = ", ".join(last_sections[:3])
        if len(last_sections) > 3:
            human += "…"
        domain_label = (domain or "").strip()
        if domain_label:
            domain_label = domain_label.lower()
            label = f"♻️ Разделы для {domain_label}: {human}"
        else:
            label = f"♻️ Разделы по умолчанию: {human}"
        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"parse|use_last|{value}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows)


def build_post_parse_extra_actions_kb() -> InlineKeyboardMarkup:
    """
    Дополнительные действия после завершения парсинга.

    Используется в паре с основным выводом результатов, отправляется отдельным
    сообщением и не ломает текущую клавиатуру.
    """

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📥 Экспорт адресов в Excel", callback_data="bulk:xls:export")],
            [InlineKeyboardButton("✏️ Отправить правки текстом", callback_data="bulk:txt:start")],
        ]
    )


def build_sections_suggest_kb(
    token: str, candidates: list[str], selected: set[str] | None
) -> InlineKeyboardMarkup:
    """Build keyboard for interactive section selection."""

    active = selected or set()
    rows: list[list[InlineKeyboardButton]] = []
    for prefix in candidates:
        mark = "✅" if prefix in active else "⬜"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{mark} {prefix}",
                    callback_data=f"sect|toggle|{token}|{prefix}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("▶️ Старт", callback_data=f"sect|run|{token}"),
            InlineKeyboardButton("✖️ Отмена", callback_data=f"sect|cancel|{token}"),
        ]
    )
    return InlineKeyboardMarkup(rows)


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


def send_flow_keyboard() -> InlineKeyboardMarkup:
    """Inline keyboard shown before starting bulk send."""

    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🚀 Отправить", callback_data="bulk:send:start")],
            [
                InlineKeyboardButton(
                    "↩️ Вернуться / Править", callback_data="bulk:send:back"
                )
            ],
            [
                InlineKeyboardButton(
                    "✏️ Исправить адрес", callback_data="bulk:send:edit"
                )
            ],
        ]
    )


def build_bulk_edit_kb(
    emails: Sequence[str], page: int = 0, page_size: int = 10
) -> InlineKeyboardMarkup:
    """Keyboard for bulk address editing actions."""

    total = len(emails)
    if page_size <= 0:
        page_size = 10
    max_page = max((total - 1) // page_size, 0) if total else 0
    page = max(0, min(page, max_page))
    start = page * page_size
    end = start + page_size
    visible = emails[start:end]

    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("➕ Добавить", callback_data="bulk:edit:add"),
            InlineKeyboardButton("🔁 Заменить", callback_data="bulk:edit:replace"),
        ]
    ]

    for email in visible:
        rows.append(
            [
                InlineKeyboardButton(
                    f"🗑 {email}", callback_data=f"bulk:edit:del:{email}"
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️", callback_data=f"bulk:edit:page:{page - 1}")
        )
    if end < total:
        nav.append(
            InlineKeyboardButton("➡️", callback_data=f"bulk:edit:page:{page + 1}")
        )
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("✅ Готово", callback_data="bulk:edit:done")])
    return InlineKeyboardMarkup(rows)
