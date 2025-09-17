"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery

from emailbot.bot import keyboards
from emailbot.settings import list_available_directions, resolve_label

router = Router()


@router.message(F.text == "/start")
async def start(message: Message) -> None:
    """Send keyboard with available directions."""

    directions = list_available_directions()
    await message.answer(
        "Выберите направление рассылки:",
        reply_markup=keyboards.directions_keyboard(directions),
    )


@router.callback_query(F.data.startswith("set_group:"))
async def set_group(callback: CallbackQuery) -> None:
    """Handle group selection from inline keyboard."""

    label = callback.data.split("set_group:", 1)[1]
    slug = resolve_label(label)
    await callback.message.answer(f"Вы выбрали: {label} ({slug})")
    await callback.answer()
