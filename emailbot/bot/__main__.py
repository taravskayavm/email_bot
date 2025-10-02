"""PTB-вход: короткий старт, категории и нижняя панель в стиле «золотого» UI."""
from __future__ import annotations
import os
from pathlib import Path

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
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
    # Короткое сообщение для /start, без зависимостей от .env
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


def _categories_inline_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🧬 Биоинформатика", callback_data="cat:bio")],
        [InlineKeyboardButton("🗺️ География", callback_data="cat:geo")],
        [InlineKeyboardButton("🧠 Психология", callback_data="cat:psy")],
        [InlineKeyboardButton("🏃 Спорт", callback_data="cat:sport")],
        [InlineKeyboardButton("🧳 Туризм", callback_data="cat:tour")],
    ]
    return InlineKeyboardMarkup(rows)


def _bottom_reply_keyboard() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("📦 Массовая"), KeyboardButton("✉️ Ручная")],
        [KeyboardButton("🧹 Очистить список"), KeyboardButton("📄 Показать исключения")],
        [KeyboardButton("ℹ️ О боте"), KeyboardButton("📊 Отчёты")],
        [KeyboardButton("⛔ Стоп")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Старт: короткий текст, категории и постоянная нижняя панель."""

    message = update.message
    if message is None:
        return

    await message.reply_text("Меню", reply_markup=_bottom_reply_keyboard())
    await message.reply_text(
        _normalize_html(_load_start_text()),
        reply_markup=_categories_inline_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_bulk_inline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    await query.message.reply_text(
        _normalize_html(_load_bulk_text()),
        reply_markup=_bottom_reply_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message and message.document:
        await message.reply_text("Файл получен. Обработка запустится отдельно.")


async def on_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()


async def on_bulk_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        _normalize_html(_load_bulk_text()),
        reply_markup=_bottom_reply_keyboard(),
        parse_mode=ParseMode.HTML,
    )


async def on_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "Остановлено. Клавиатура скрыта.", reply_markup=ReplyKeyboardRemove()
    )


async def on_show_exclusions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "Исключения пока пусты.", reply_markup=_bottom_reply_keyboard()
    )


async def on_manual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "Ручной режим скоро вернём (заглушка).",
        reply_markup=_bottom_reply_keyboard(),
    )


async def on_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "Список очищен (заглушка).", reply_markup=_bottom_reply_keyboard()
    )


async def on_about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "О боте: PTB-версия без aiogram.", reply_markup=_bottom_reply_keyboard()
    )


async def on_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    if message is None:
        return
    await message.reply_text(
        "Отчёты будут здесь (заглушка).",
        reply_markup=_bottom_reply_keyboard(),
    )


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

    app.add_handler(CallbackQueryHandler(on_bulk_inline, pattern=r"^bulk:start$"))
    app.add_handler(CallbackQueryHandler(on_category, pattern=r"^cat:(bio|geo|psy|sport|tour)$"))

    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^📦 Массовая$"), on_bulk_reply)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^⛔ Стоп$"), on_stop)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(r"^📄 Показать исключения$"),
            on_show_exclusions,
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^✉️ Ручная$"), on_manual)
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(r"^🧹 Очистить список$"), on_clear
        )
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^ℹ️ О боте$"), on_about)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.Regex(r"^📊 Отчёты$"), on_reports)
    )

    app.add_handler(MessageHandler(filters.Document.ALL, on_document))

    # run_polling — синхронный, сам управляет инициализацией/остановкой
    # close_loop=False — чтобы не ломать чужой event loop на Windows
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main_sync()
