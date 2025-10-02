"""Start/help handlers for the aiogram-based bot."""

from __future__ import annotations

import os
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


def _load_start_message_text() -> str:
    """
    Возвращает текст приветствия:
      1) если задан START_MESSAGE_HTML_PATH и файл существует — читаем из него;
      2) иначе, если задан START_MESSAGE_TEXT — используем его (можно с HTML);
      3) иначе — дефолтный текст (текущий).
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


@router.message(CommandStart())
@router.message(Command("help"))
async def start(message: Message) -> None:
    """Reply with instructions and optional inline keyboard of directions."""

    text = _load_start_message_text()
    show_directions = os.getenv("START_MESSAGE_SHOW_DIRECTIONS", "1") == "1"
    keyboard = None
    if show_directions:
        directions = list_available_directions()
        if directions:
            keyboard = keyboards.directions_keyboard(directions)

    # В aiogram parse_mode уже HTML (ставится в __main__), поэтому можно присылать HTML.
    await message.answer(text, reply_markup=keyboard)
