"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery

from emailbot.settings import resolve_label

router = Router()


@router.callback_query(F.data.startswith("set_group:"))
async def set_group(callback: CallbackQuery) -> None:
    """Handle group selection from inline keyboard."""

    label = callback.data.split("set_group:", 1)[1]
    slug = resolve_label(label)
    await callback.message.answer(f"Вы выбрали: {label} ({slug})")
    await callback.answer()
