"""Start/help handlers for the aiogram-based bot."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from emailbot.bot import keyboards
from emailbot.settings import list_available_directions

router = Router()


START_MESSAGE = "<b>Можно загрузить данные</b>"


def _help_message() -> str:
    return (
        "Привет! Я помогу с рассылкой.\n\n"
        "• /send email@domain.tld | Тема | Текст — отправка письма вручную.\n"
        "• Выберите направление из списка ниже, чтобы работать с шаблонами.\n\n"
        "Правило: один адрес — не чаще раза в 180 дней."
    )


def _build_keyboard():
    directions = list_available_directions()
    if directions:
        return keyboards.directions_keyboard(directions)
    return None


@router.message(CommandStart())
async def start(message: Message) -> None:
    """Reply with a short start message and optional directions keyboard."""

    await message.answer(START_MESSAGE, reply_markup=_build_keyboard())


@router.message(Command("help"))
async def help_command(message: Message) -> None:
    """Reply with detailed help instructions and optional keyboard."""

    await message.answer(_help_message(), reply_markup=_build_keyboard())
