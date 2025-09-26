"""Start/help handlers for the aiogram-based bot."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from emailbot.bot import keyboards
from emailbot.settings import list_available_directions

router = Router()


def _start_message() -> str:
    return (
        "Привет! Я помогу с рассылкой.\n\n"
        "• /send email@domain.tld | Тема | Текст — отправка письма вручную.\n"
        "• Выберите направление из списка ниже, чтобы работать с шаблонами.\n\n"
        "Правило: один адрес — не чаще раза в 180 дней."
    )


@router.message(CommandStart())
@router.message(Command("help"))
async def start(message: Message) -> None:
    """Reply with instructions and inline keyboard of directions."""

    directions = list_available_directions()
    keyboard = None
    if directions:
        keyboard = keyboards.directions_keyboard(directions)
    await message.answer(_start_message(), reply_markup=keyboard)
