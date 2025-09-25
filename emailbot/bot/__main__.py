"""Async entrypoint for the aiogram-based Telegram bot."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand
from dotenv import load_dotenv

from emailbot.bot.handlers.ingest import router as ingest_router
from emailbot.bot.handlers.send import router as send_router
from emailbot.bot.handlers.start import router as start_router


def _load_dotenv() -> None:
    env_file = Path(".env")
    if env_file.exists():
        load_dotenv(env_file)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _resolve_token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if token:
        return token
    try:
        import emailbot.settings as settings_module  # type: ignore

        value = getattr(settings_module, "TELEGRAM_BOT_TOKEN", None)
        if value:
            return str(value)
    except Exception:
        pass
    raise SystemExit("TELEGRAM_BOT_TOKEN is not set (check .env)")


async def _set_bot_commands(bot: Bot) -> None:
    commands = [
        BotCommand(command="start", description="Запуск меню"),
        BotCommand(command="help", description="Краткая инструкция"),
        BotCommand(command="send", description="Ручная отправка письма"),
    ]
    try:
        await bot.set_my_commands(commands)
    except Exception:
        logging.getLogger(__name__).debug("Unable to set bot commands", exc_info=True)


async def main() -> None:
    """Run the bot dispatcher until cancelled."""

    _load_dotenv()
    _setup_logging()
    token = _resolve_token()
    bot = Bot(token=token, parse_mode="HTML")
    dispatcher = Dispatcher()
    dispatcher.include_router(start_router)
    dispatcher.include_router(ingest_router)
    dispatcher.include_router(send_router)
    await _set_bot_commands(bot)
    await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        try:  # pragma: no cover - specific to Windows event loop
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    asyncio.run(main())
