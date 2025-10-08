"""Async entrypoint for the aiogram-based Telegram bot."""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import pkgutil
import signal
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher, Router
from aiogram.types import BotCommand

from emailbot.run_control import clear_stop, request_stop

try:  # pragma: no cover - optional dependency
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency

    def load_dotenv(*args, **kwargs):
        return False

def _make_bot(token: str) -> Bot:
    """
    Создаёт Bot корректно для aiogram <3.7 и >=3.7.
    UI/логика не меняются (HTML по умолчанию).
    """

    try:
        from aiogram.client.default import DefaultBotProperties
        from aiogram.enums import ParseMode

        return Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    except Exception:
        return Bot(token=token, parse_mode="HTML")


def _load_dotenv() -> None:
    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
    else:
        load_dotenv()


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
    raise SystemExit("TELEGRAM_BOT_TOKEN is not set. Specify it in .env or environment.")


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


def include_all_routers(dp: Dispatcher) -> None:
    """Auto-import and include all routers from ``emailbot.bot.handlers``."""

    from emailbot.bot import handlers as handlers_pkg

    pkg_path = Path(handlers_pkg.__file__).parent
    for module_info in pkgutil.iter_modules([str(pkg_path)]):
        module_name = module_info.name
        if module_name.startswith("_"):
            continue
        module = importlib.import_module(f"{handlers_pkg.__name__}.{module_name}")
        router = getattr(module, "router", None)
        if not isinstance(router, Router):
            for attr_name, value in vars(module).items():
                if attr_name.endswith("_router") and isinstance(value, Router):
                    router = value
                    break
        if isinstance(router, Router):
            dp.include_router(router)


async def main() -> None:
    """Run the bot dispatcher until cancelled."""

    _load_dotenv()
    _setup_logging()
    token = _resolve_token()
    bot = _make_bot(token)
    dispatcher = Dispatcher()
    clear_stop()
    try:
        from emailbot.bot.middlewares.error_logging import ErrorLoggingMiddleware

        dispatcher.message.middleware(ErrorLoggingMiddleware())
        dispatcher.callback_query.middleware(ErrorLoggingMiddleware())
    except Exception:
        pass
    include_all_routers(dispatcher)
    await _set_bot_commands(bot)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: (request_stop(), stop_event.set()))
        except NotImplementedError:  # pragma: no cover - specific to Windows/embedded loops
            # Windows Py<3.8 и некоторые окружения
            pass

    async with bot:
        polling = asyncio.create_task(
            dispatcher.start_polling(
                bot,
                allowed_updates=dispatcher.resolve_used_update_types(),
            )
        )
        polling.add_done_callback(lambda _: (request_stop(), stop_event.set()))
        await stop_event.wait()
        if not polling.done():
            polling.cancel()
        try:
            await polling
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        try:  # pragma: no cover - specific to Windows event loop
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
        except Exception:
            pass
    asyncio.run(main())
