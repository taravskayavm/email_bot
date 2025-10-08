"""Handlers for stopping background tasks via the Telegram UI."""

from __future__ import annotations

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from emailbot.run_control import stop_and_status

router = Router()


def _format_status(payload: dict) -> str:
    running = payload.get("running") or {}
    if not running:
        return "🛑 Останавливаю все процессы…\nАктивных задач не осталось."
    lines = ["🛑 Останавливаю все процессы…", "Текущие задачи:"]
    for name, info in sorted(running.items()):
        lines.append(f"• {name}: {info}")
    return "\n".join(lines)


@router.callback_query(F.data == "stop_all")
async def cb_stop_all(call: CallbackQuery) -> None:
    status = stop_and_status()
    await call.message.answer(_format_status(status))
    await call.answer()


@router.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    status = stop_and_status()
    await message.answer(_format_status(status))


@router.message(lambda m: (m.text or "").strip().lower() in {"стоп", "🛑 стоп"})
async def text_stop(message: Message) -> None:
    status = stop_and_status()
    await message.answer(_format_status(status))
