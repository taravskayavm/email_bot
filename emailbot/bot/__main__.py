"""Aiogram-based entry point for running the Telegram email bot."""

# Импортируем поддержку будущего поведения аннотаций для единообразных типов.
from __future__ import annotations

# Импортируем asyncio для запуска асинхронной точки входа.
import asyncio
# Импортируем logging для базовой настройки логов.
import logging
# Импортируем os для чтения переменных окружения.
import os

# Импортируем объекты Bot и Dispatcher из aiogram для работы бота.
from aiogram import Bot, Dispatcher

# Импортируем существующие хендлеры /start и /stop.
from emailbot.bot.handlers import start as start_handlers
from emailbot.bot.handlers import stop as stop_handlers


# Имя переменной окружения с токеном Telegram-бота.
TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"


async def main() -> None:
    """Основная точка входа aiogram-бота."""

    # Настраиваем базовое логирование согласно требованиям задачи.
    logging.basicConfig(
        level=logging.INFO,  # Логируем события уровня INFO и выше.
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # Формат сообщения.
    )

    # Получаем токен из переменной окружения.
    token = os.getenv(TELEGRAM_BOT_TOKEN_ENV)
    # Проверяем, что токен задан.
    if not token:
        # Выбрасываем RuntimeError с понятным описанием.
        raise RuntimeError(
            f"{TELEGRAM_BOT_TOKEN_ENV} is not set in environment. "
            "Please configure your .env before running the aiogram bot."
        )

    # Создаем экземпляр Bot aiogram с полученным токеном.
    bot = Bot(token=token)
    # Создаем диспетчер для регистрации роутеров и запуска polling.
    dp = Dispatcher()

    # Подключаем роутер с командами /start и /help.
    dp.include_router(start_handlers.router)
    # Подключаем роутер с командами /stop и связанными обработчиками.
    dp.include_router(stop_handlers.router)

    # Логируем, что бот запускается через long polling.
    logging.getLogger(__name__).info(
        "Starting aiogram Email Bot via long polling"
    )
    # Запускаем long polling и ожидаем завершения.
    await dp.start_polling(bot)


if __name__ == "__main__":
    # Запускаем асинхронную функцию main через asyncio.run.
    asyncio.run(main())
