"""User interface helpers for inline keyboards (Telegram)."""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from emailbot.config import ENABLE_INLINE_EMAIL_EDITOR

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


groups_map = {
    "bioinformatics": "Биоинформатика",
    "geography": "География",
    "psychology": "Психология",
    "beauty": "Индустрия красоты",
    "medicine": "Медицина",
    "sport": "Спорт",
    "tourism": "Туризм",
}


def build_after_parse_combined_kb(
    extra_rows: Sequence[Sequence[InlineKeyboardButton]] | None = None,
    *,
    is_admin: bool = True,
) -> InlineKeyboardMarkup:
    """Keyboard shown after parsing with follow-up actions."""

    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("👀 Показать примеры", callback_data="refresh_preview")],
        [
            InlineKeyboardButton(
                "🧭 Перейти к выбору направления",
                callback_data="proceed_group",
            )
        ],
        [
            InlineKeyboardButton(
                "✏️ Отправить правки текстом",
                callback_data="bulk:txt:start",
            )
        ],
    ]
    if ENABLE_INLINE_EMAIL_EDITOR and is_admin:
        rows.append(
            [
                InlineKeyboardButton(
                    "✏️ Исправить адреса (встроенно)",
                    callback_data="bulk:edit:start",
                )
            ]
        )
    if extra_rows:
        rows.extend(extra_rows)
    return InlineKeyboardMarkup(rows)


def build_bulk_edit_kb(
    emails: Sequence[str],
    page: int = 0,
    page_size: int = 10,
) -> InlineKeyboardMarkup:
    """Keyboard for paginated bulk e-mail editing."""

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


def build_skipped_preview_entry_kb() -> InlineKeyboardMarkup:
    """Keyboard entry point for skipped-address previews."""

    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("👀 Показать примеры", callback_data="skipped_menu")]]
    )


def build_skipped_preview_kb() -> InlineKeyboardMarkup:
    """Keyboard with quick-access buttons for skipped e-mail categories."""

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "👀 Примеры: 180 дней", callback_data="skipped:180d"
                ),
                InlineKeyboardButton(
                    "👀 Примеры: сегодня", callback_data="skipped:today"
                ),
            ],
            [
                InlineKeyboardButton(
                    "👀 Примеры: кулдаун", callback_data="skipped:cooldown"
                ),
                InlineKeyboardButton(
                    "👀 Примеры: роль/служебные",
                    callback_data="skipped:blocked_role",
                ),
            ],
            [
                InlineKeyboardButton(
                    "👀 Примеры: загр. домены",
                    callback_data="skipped:blocked_foreign",
                ),
                InlineKeyboardButton(
                    "👀 Примеры: невалидные", callback_data="skipped:invalid"
                ),
            ],
        ]
    )
