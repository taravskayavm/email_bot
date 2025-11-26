"""Inline keyboards used by the aiogram-based bot."""

from __future__ import annotations

import json
import os
import unicodedata
from pathlib import Path

# Импортируем типы aiogram для создания кнопок и клавиатур
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from emailbot.ui.callbacks import (  # Импортируем константы колбэков для единообразия
    CB_EXAMPLES_BACK,  # Константа для возврата к отчёту
    CB_EXAMPLES_INIT,  # Константа для открытия примеров
    CB_EXAMPLES_MORE_COOLDOWN,  # Константа для догрузки кулдауна
    CB_EXAMPLES_MORE_FOREIGN,  # Константа для догрузки иностранных адресов
)

def _resolve_icons_path() -> Path:
    override = os.getenv("DIRECTION_ICONS_PATH")
    if override:
        return Path(os.path.expandvars(os.path.expanduser(override))).resolve()

    module_dir = Path(__file__).resolve().parent
    module_local = module_dir / "icons.json"
    if module_local.exists():
        return module_local

    return module_dir.parents[1] / "icons.json"


ICONS_PATH = _resolve_icons_path()
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
    builder.button(text="🛑 Стоп", callback_data="stop_all")
    builder.adjust(1)
    return builder.as_markup()


def build_examples_entry_kb() -> InlineKeyboardMarkup:
    """Keyboard with a single button that opens the examples list."""

    builder = InlineKeyboardBuilder()  # Создаём билдер для инлайн-клавиатуры
    builder.button(  # Добавляем кнопку запуска примеров с использованием константы
        text="👀 Показать примеры отфильтрованных",  # Подпись кнопки с уточнением
        callback_data=CB_EXAMPLES_INIT,  # Привязываем колбэк открытия примеров
    )
    builder.adjust(1)  # Располагаем кнопку в одной строке
    return builder.as_markup()  # Возвращаем готовую клавиатуру


def build_examples_paging_kb() -> InlineKeyboardMarkup:
    """Keyboard with pagination controls for the examples list."""

    builder = InlineKeyboardBuilder()  # Создаём билдер для набора кнопок управления
    builder.button(
        text="🔁 Показать ещё 180 дней",  # Подпись для кнопки догрузки кулдауна
        callback_data=CB_EXAMPLES_MORE_COOLDOWN,  # Привязываем обработчик для догрузки кулдауна
    )
    builder.button(
        text="🔁 Показать ещё иностранные",  # Подпись для кнопки догрузки иностранных адресов
        callback_data=CB_EXAMPLES_MORE_FOREIGN,  # Привязываем обработчик для иностранных адресов
    )
    builder.button(
        text="⬅️ Назад к отчёту",  # Подпись для возврата к исходному отчёту
        callback_data=CB_EXAMPLES_BACK,  # Привязываем обработчик возврата
    )
    builder.adjust(1)  # Каждая кнопка будет находиться на отдельной строке
    return builder.as_markup()  # Возвращаем готовую клавиатуру


def kb_examples_entry() -> InlineKeyboardMarkup:
    """Wrapper returning the examples entry keyboard for compatibility."""  # Объясняем назначение функции

    return build_examples_entry_kb()  # Возвращаем ранее определённую клавиатуру входа в примеры


def kb_examples_paging() -> InlineKeyboardMarkup:
    """Wrapper returning the examples paging keyboard for compatibility."""  # Объясняем назначение функции

    return build_examples_paging_kb()  # Возвращаем ранее определённую клавиатуру пагинации примеров
