"""Основной модуль aiogram-бота с базовыми командами."""

import asyncio  # Импортируем asyncio для запуска event loop.
import logging  # Импортируем logging для настройки логирования.
import os  # Импортируем os для чтения переменных окружения.

from aiogram import Bot, Dispatcher  # Импортируем Bot и Dispatcher из aiogram.
from aiogram.filters import Command, CommandStart  # Импортируем фильтры команд.
from aiogram.types import Message  # Импортируем тип Message для аннотаций.


TELEGRAM_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"  # Имя переменной окружения с токеном.


async def cmd_start(message: Message) -> None:
    """Отвечает на /start и объясняет основные команды."""

    text = (  # Формируем текст с описанием возможностей бота.
        "Привет! Это Email Bot для поиска контактов и рассылки писем.\n\n"
        "Доступные команды парсинга сайтов:\n"
        "/url <ссылка> — парсинг одной страницы (поиск e-mail на этой странице).\n"
        "/crawl <ссылка> — глубокий обход сайта с поиском контактов на нескольких страницах.\n\n"
        "Примеры:\n"
        "/url https://example.com/page\n"
        "/crawl https://example.com --max-pages 80 --max-depth 3\n"
        "/crawl https://example.com --prefix /staff,/contacts\n"
    )
    await message.answer(text)  # Отправляем сформированный ответ пользователю.


async def cmd_help(message: Message) -> None:
    """Отвечает на /help и даёт инструкции по парсингу."""

    text = (  # Формируем текст подсказки по командам /url и /crawl.
        "Справка по парсингу сайтов:\n\n"
        "Одна страница:\n"
        "    /url https://example.com/page\n\n"
        "Глубокий обход (несколько страниц):\n"
        "    /crawl https://example.com\n"
        "    /crawl https://example.com --max-pages 80 --max-depth 3\n"
        "    /crawl https://example.com --prefix /staff,/contacts\n\n"
        "Дальнейшая логика (поиск e-mail, фильтры, рассылка) остаётся такой же, как в текущем боте."
    )
    await message.answer(text)  # Отправляем подсказку пользователю.


def build_dispatcher() -> Dispatcher:
    """Создаёт Dispatcher и регистрирует хендлеры команд."""

    dp = Dispatcher()  # Создаём диспетчер aiogram.
    dp.message.register(cmd_start, CommandStart())  # Регистрируем обработчик /start.
    dp.message.register(cmd_help, Command(commands=["help"]))  # Регистрируем обработчик /help.
    return dp  # Возвращаем готовый диспетчер.


async def main() -> None:
    """Запускает aiogram-бота через long polling."""

    logging.basicConfig(  # Настраиваем базовое логирование для приложения.
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    token = os.getenv(TELEGRAM_BOT_TOKEN_ENV)  # Получаем токен бота из окружения.
    if not token:  # Проверяем, что токен задан.
        raise RuntimeError(  # Сообщаем об отсутствующем токене.
            f"{TELEGRAM_BOT_TOKEN_ENV} is not set in environment. "
            "Please configure your .env before running aiogram bot."
        )

    bot = Bot(token=token)  # Создаём экземпляр Bot с прочитанным токеном.
    dp = build_dispatcher()  # Конструируем диспетчер с зарегистрированными командами.

    logging.getLogger(__name__).info(  # Логируем запуск long polling.
        "Starting aiogram Email Bot via long polling"
    )
    await dp.start_polling(bot)  # Запускаем обработку обновлений aiogram.


if __name__ == "__main__":  # Проверяем, что файл запущен как скрипт.
    asyncio.run(main())  # Запускаем асинхронный main через asyncio.run.
