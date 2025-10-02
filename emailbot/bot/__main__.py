"""
PTB-вход: /start -> короткое «Можно загрузить данные» + одна inline «Массовая».
Клик «Массовая» -> отдельное сообщение с подробной инструкцией.
Никакого aiogram.
"""
from __future__ import annotations
import os
from pathlib import Path

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


def _load_env() -> None:
    root = Path(__file__).resolve().parents[2]
    env = root / ".env"
    load_dotenv(dotenv_path=env if env.exists() else None)


def _read_file(path: str) -> str | None:
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None
    try:
        return file_path.read_text(encoding="utf-8")
    except Exception:
        return None


def _load_start_text() -> str:
    # фиксируем короткий старт, без зависимостей от .env
    return "<b>Можно загрузить данные</b>"


def _load_bulk_text() -> str:
    # подробная инструкция после клика «Массовая»
    text = _read_file((os.getenv("BULK_MESSAGE_HTML_PATH") or "").strip())
    if text:
        return text
    env_value = (os.getenv("BULK_MESSAGE_TEXT") or "").strip()
    if env_value:
        return env_value
    return (
        "Загрузите данные со e-mail-адресами для рассылки.\n\n"
        "Поддерживаемые форматы: PDF, Excel (xlsx), Word (docx), CSV, "
        "ZIP (с этими файлами внутри), а также ссылки на сайты.\n\n"
        "<i>Примечание:</i>\n"
        "Мы удаляем возможные сноски/обфускации и проверяем адреса (MX/дубликаты).\n"
        "Правило: один адрес — не чаще раза в 180 дней."
    )


_ALLOWED_TAGS = "b|strong|i|em|u|ins|s|strike|del|a|code|pre|tg-spoiler"


def _normalize_html(text: str) -> str:
    import re

    if not text:
        return text
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(
        rf"</?(?!{_ALLOWED_TAGS})([a-z0-9:-]+)(?:\s[^>]*)?>",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text


def _start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup.from_button(
        InlineKeyboardButton("Массовая", callback_data="bulk:start")
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # убираем любую висящую reply-клавиатуру и показываем короткий старт
    message = update.message
    if message is None:
        return
    try:
        await message.reply_text(" ", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass
    await message.reply_text(
        _normalize_html(_load_start_text()),
        reply_markup=_start_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    await query.message.reply_text(
        _normalize_html(_load_bulk_text()),
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.HTML,
    )


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message and message.document:
        await message.reply_text("Файл получен. Обработка запустится отдельно.")


def main_sync() -> None:
    """
    Корректный запуск PTB v20+: без прямой работы с Updater.
    Просто регистрируем хэндлеры и вызываем run_polling().
    """

    _load_env()
    token = os.getenv("TELEGRAM_BOT_TOKEN") or ""
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN не задан в .env")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CallbackQueryHandler(on_bulk, pattern=r"^bulk:start$"))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    # run_polling — синхронный, сам управляет инициализацией/остановкой
    # close_loop=False — чтобы не ломать чужой event loop на Windows
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main_sync()
