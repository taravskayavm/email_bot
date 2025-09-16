from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from services.templates import list_templates


def build_templates_kb(
    current_code: str | None = None, prefix: str = "tpl:"
) -> InlineKeyboardMarkup:
    rows = []
    for t in list_templates():
        label = t["label"] + (" • текущий" if t["code"] == current_code else "")
        rows.append(
            [InlineKeyboardButton(label, callback_data=f"{prefix}{t['code']}")]
        )
    return InlineKeyboardMarkup(rows)
