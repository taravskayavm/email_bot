from __future__ import annotations

from aiogram import F, Router, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from services.ui_state import foreign_allowed_for_batch, toggle_foreign_for_batch


router = Router()


def make_actions_kb(batch_id: str, chat_id: int) -> InlineKeyboardMarkup:
    state = "✅ включены" if foreign_allowed_for_batch(batch_id, chat_id) else "🚫 выключены"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Отправить правки текстом",
                    callback_data=f"edit_text:{batch_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить отдельные адреса",
                    callback_data=f"delete_some:{batch_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"Иностранные домены: {state}",
                    callback_data=f"toggle_foreign:{batch_id}",
                )
            ],
        ]
    )


@router.callback_query(F.data.startswith("delete_some:"))
async def on_delete_some(callback: types.CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data.startswith("toggle_foreign:"))
async def on_toggle_foreign(callback: types.CallbackQuery) -> None:
    data = callback.data or ""
    batch_id = data.split(":", 1)[1] if ":" in data else ""
    message = callback.message
    chat_id = message.chat.id if message else None
    if not batch_id:
        await callback.answer("Некорректный запрос", show_alert=True)
        return

    new_state = toggle_foreign_for_batch(batch_id, chat_id)

    if message and chat_id is not None:
        try:
            kb = make_actions_kb(batch_id, chat_id)
            await message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

    await callback.answer(
        "Иностранные домены " + ("включены" if new_state else "выключены")
    )


__all__ = ["router", "make_actions_kb"]
