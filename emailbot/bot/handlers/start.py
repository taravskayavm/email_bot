"""Start/help handlers for the aiogram-based bot (configurable /start message)."""

from __future__ import annotations

import os
import re
from pathlib import Path

from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from emailbot.bot import keyboards
from emailbot.settings import list_available_directions

router = Router()
DEFAULT_START_MESSAGE = (
    "Привет! Я помогу с рассылкой.\n\n"
    "• /send email@domain.tld | Тема | Текст — отправка письма вручную.\n"
    "• Выберите направление из списка ниже, чтобы работать с шаблонами.\n\n"
    "Правило: один адрес — не чаще раза в 180 дней."
)

_ALLOWED_TELEGRAM_HTML_TAGS = (
    "b",
    "strong",
    "i",
    "em",
    "u",
    "ins",
    "s",
    "strike",
    "del",
    "a",
    "code",
    "pre",
    "tg-spoiler",
)
_ALLOWED_TELEGRAM_TAGS_PATTERN = "|".join(_ALLOWED_TELEGRAM_HTML_TAGS)


def _load_start_message_text() -> str:
    """
    Порядок приоритета:
      1) START_MESSAGE_HTML_PATH указывает на существующий файл — читаем его;
      2) иначе START_MESSAGE_TEXT из .env (строка; можно HTML);
      3) иначе дефолтный текст (как сейчас).
    """

    path_value = (os.getenv("START_MESSAGE_HTML_PATH") or "").strip()
    if path_value:
        path = Path(path_value)
        if path.exists():
            try:
                return path.read_text(encoding="utf-8")
            except Exception:
                pass

    env_text = (os.getenv("START_MESSAGE_TEXT") or "").strip()
    if env_text:
        return env_text

    return DEFAULT_START_MESSAGE


def _normalize_telegram_html(text: str) -> str:
    """Нормализует произвольный текст/HTML к допустимому Telegram HTML."""

    if not text:
        return text

    normalized = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    normalized = re.sub(
        rf"</?(?!{_ALLOWED_TELEGRAM_TAGS_PATTERN})([a-z0-9:-]+)(?:\s[^>]*)?>",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalized


@router.message(CommandStart())
@router.message(Command("help"))
async def start(message: Message) -> None:
    """Reply with configurable instructions and optional directions keyboard."""

    raw_text = _load_start_message_text()
    text = _normalize_telegram_html(raw_text)
    show_directions = os.getenv("START_MESSAGE_SHOW_DIRECTIONS", "1") == "1"
    keyboard = None
    if show_directions:
        directions = list_available_directions()
        if directions:
            keyboard = keyboards.directions_keyboard(directions)

    # parse_mode=HTML задаётся в __main__, так что можно присылать HTML
    await message.answer(text, reply_markup=keyboard)
