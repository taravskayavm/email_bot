from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from services.templates import list_templates


def build_templates_kb(
    current_code: str | None = None,
    *,
    prefix: str = "tpl:",
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    normalized_current = (current_code or "").strip().lower()
    for template in list_templates():
        code = template.get("code", "")
        label = str(template.get("label") or code)
        display = label
        if normalized_current and code.strip().lower() == normalized_current:
            display = f"{label} • текущий"
        rows.append([InlineKeyboardButton(display, callback_data=f"{prefix}{code}")])
    if not rows:
        rows.append(
            [
                InlineKeyboardButton(
                    "Нет доступных шаблонов", callback_data=f"{prefix}none"
                )
            ]
        )
    return InlineKeyboardMarkup(rows)
