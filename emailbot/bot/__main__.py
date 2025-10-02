"""
Telegram-бот на python-telegram-bot (PTB), без aiogram.
Запускается из email_bot.py.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from emailbot.settings import list_available_directions


# --------- загрузка .env ----------
def _load_env() -> None:
    # .env в корне репозитория
    here = Path(__file__).resolve().parents[2]
    env_path = here / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()


# --------- текст приветствия ----------
def _load_start_message_text() -> str:
    """
    Приоритет:
      1) START_MESSAGE_HTML_PATH -> прочитать файл
      2) START_MESSAGE_TEXT из .env
      3) дефолтный текст
    """

    path = (os.getenv("START_MESSAGE_HTML_PATH") or "").strip()
    if path:
        p = Path(path)
        if p.exists():
            try:
                return p.read_text(encoding="utf-8")
            except Exception:
                pass
    env_text = (os.getenv("START_MESSAGE_TEXT") or "").strip()
    if env_text:
        return env_text
    # дефолт: краткий «старый» экран
    return (
        "<b>Можно загрузить данные</b>\n\n"
        "Загрузите данные со e-mail-адресами для рассылки.\n\n"
        "Поддерживаемые форматы: PDF, Excel (xlsx), Word (docx), CSV,\n"
        "ZIP (с этими файлами внутри), а также ссылки на сайты.\n\n"
        "<i>Примечание:</i>\n"
        "Мы удаляем возможные сноски/обфускации и проверяем адреса (MX/дубликаты).\n"
        "Правило: один адрес — не чаще раза в 180 дней."
    )


_ALLOWED_HTML_TAGS = (
    "b|strong|i|em|u|ins|s|strike|del|a|code|pre|tg-spoiler"
)


def _normalize_telegram_html(text: str) -> str:
    """Удаляем неподдерживаемые теги для HTML-режима Bot API; <br> -> \n."""

    import re

    if not text:
        return text
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(
        rf"</?(?!{_ALLOWED_HTML_TAGS})([a-z0-9:-]+)(?:\s[^>]*)?>",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text


# --------- клавиатура направлений ----------
def _directions_keyboard() -> InlineKeyboardMarkup | None:
    dirs = list_available_directions()
    if not dirs:
        return None
    buttons = [[InlineKeyboardButton(text=d, callback_data=f"dir:{d}")] for d in dirs]
    return InlineKeyboardMarkup(buttons)


# --------- хэндлеры ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = _normalize_telegram_html(_load_start_message_text())
    show_dirs = os.getenv("START_MESSAGE_SHOW_DIRECTIONS", "1") == "1"
    keyboard = _directions_keyboard() if show_dirs else None
    if update.message:
        await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)


async def on_direction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data or ""
    if data.startswith("dir:"):
        direction = data.split(":", 1)[1]
        message = query.message
        if message:
            await message.reply_text(
                "Выбрано направление: <b>{direction}</b>\n"
                "Отправьте файл (PDF/DOCX/XLSX/CSV или ZIP) или ссылку на сайт.".format(
                    direction=direction
                ),
                parse_mode=ParseMode.HTML,
            )


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Минимальный приём файла, без обработки — бизнес-логика остаётся в пайплайнах.
    if update.message and update.message.document:
        await update.message.reply_text("Файл получен. Обработка запустится отдельно.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)


def _resolve_token() -> str:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if token:
        return token
    try:
        import emailbot.settings as settings

        if getattr(settings, "TELEGRAM_BOT_TOKEN", None):
            return str(settings.TELEGRAM_BOT_TOKEN)
    except Exception:
        pass
    raise SystemExit("TELEGRAM_BOT_TOKEN не задан. Укажите в .env")


def main() -> None:
    _load_env()
    token = _resolve_token()
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CallbackQueryHandler(on_direction, pattern=r"^dir:"))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
