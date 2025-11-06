from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes

from emailbot import config
from emailbot.runtime_config import clear, set_many


PROFILE_KEYS = (
    "PDF_ADAPTIVE_TIMEOUT",
    "PDF_TIMEOUT_BASE",
    "PDF_TIMEOUT_PER_MB",
    "PDF_TIMEOUT_MIN",
    "PDF_TIMEOUT_MAX",
    "EMAILBOT_ENABLE_OCR",
    "PDF_MAX_PAGES",
)


PROFILES = {
    "fast": {
        "PDF_ADAPTIVE_TIMEOUT": True,
        "PDF_TIMEOUT_BASE": 12,
        "PDF_TIMEOUT_PER_MB": 0.5,
        "PDF_TIMEOUT_MIN": 12,
        "PDF_TIMEOUT_MAX": 60,
        "EMAILBOT_ENABLE_OCR": False,
        "PDF_MAX_PAGES": 40,
    },
    "universal": {
        "PDF_ADAPTIVE_TIMEOUT": True,
        "PDF_TIMEOUT_BASE": 15,
        "PDF_TIMEOUT_PER_MB": 0.6,
        "PDF_TIMEOUT_MIN": 15,
        "PDF_TIMEOUT_MAX": 90,
        "EMAILBOT_ENABLE_OCR": False,
        "PDF_MAX_PAGES": 40,
    },
    "heavy": {
        "PDF_ADAPTIVE_TIMEOUT": True,
        "PDF_TIMEOUT_BASE": 18,
        "PDF_TIMEOUT_PER_MB": 0.7,
        "PDF_TIMEOUT_MIN": 18,
        "PDF_TIMEOUT_MAX": 120,
        "EMAILBOT_ENABLE_OCR": True,
        "PDF_MAX_PAGES": 80,
    },
}


def _current_values() -> dict[str, object]:
    return {key: getattr(config, key) for key in PROFILE_KEYS}


def _kb(ocr_on: bool) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("🚀 Быстрый", callback_data="profile:set:fast"),
            InlineKeyboardButton("⚖️ Универсальный", callback_data="profile:set:universal"),
            InlineKeyboardButton("🧱 Тяжёлый", callback_data="profile:set:heavy"),
        ],
        [
            InlineKeyboardButton(
                "🧠 OCR: Вкл" if ocr_on else "🧠 OCR: Выкл",
                callback_data="profile:toggle_ocr",
            ),
            InlineKeyboardButton("♻️ Сбросить", callback_data="profile:reset"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def _profile_text(vals: dict[str, object]) -> str:
    ocr = bool(vals.get("EMAILBOT_ENABLE_OCR") or False)
    return (
        "⚙️ *Профили скорости обработки PDF*\n\n"
        "Выберите режим под текущую задачу:\n\n"
        "🚀 *Быстрый* — короткие статьи (до 10–20 стр.).\n"
        " • Минимальный таймаут, без OCR. Самый быстрый.\n\n"
        "⚖️ *Универсальный* — по умолчанию.\n"
        " • Баланс скорости и полноты, OCR выключен.\n\n"
        "🧱 *Тяжёлый* — большие файлы/сканы.\n"
        " • Больше таймаут, OCR включён. Медленнее, но находит больше.\n\n"
        "📄 *Текущие параметры*\n"
        f" • База: {vals.get('PDF_TIMEOUT_BASE')} c; + {vals.get('PDF_TIMEOUT_PER_MB')} c/МБ\n"
        f" • Диапазон: {vals.get('PDF_TIMEOUT_MIN')}–{vals.get('PDF_TIMEOUT_MAX')} c\n"
        f" • PDF_MAX_PAGES: {vals.get('PDF_MAX_PAGES')}\n"
        f" • OCR: {'включён' if ocr else 'выключен'}\n"
    )


async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    vals = _current_values()
    await message.reply_text(
        _profile_text(vals),
        reply_markup=_kb(bool(vals.get("EMAILBOT_ENABLE_OCR"))),
        parse_mode="Markdown",
    )


async def cb_set_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    key = query.data.split(":")[-1] if query.data else ""
    cfg = PROFILES.get(key)
    if not cfg:
        await query.edit_message_text("Неизвестный профиль.")
        return
    set_many(cfg)
    vals = _current_values()
    await query.edit_message_text(
        f"✅ Профиль «{key}» применён.\n\n" + _profile_text(vals),
        reply_markup=_kb(bool(vals.get("EMAILBOT_ENABLE_OCR"))),
        parse_mode="Markdown",
    )


async def cb_toggle_ocr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer("OCR переключён")
    current = bool(getattr(config, "EMAILBOT_ENABLE_OCR"))
    set_many({"EMAILBOT_ENABLE_OCR": (not current)})
    vals = _current_values()
    await query.edit_message_text(
        "⚙️ Настройки обновлены.\n\n" + _profile_text(vals),
        reply_markup=_kb(bool(vals.get("EMAILBOT_ENABLE_OCR"))),
        parse_mode="Markdown",
    )


async def cb_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer("Сброс к .env")
    clear(list(PROFILE_KEYS))
    vals = _current_values()
    await query.edit_message_text(
        "♻️ Сброс к значениям по умолчанию (.env).\n\n" + _profile_text(vals),
        reply_markup=_kb(bool(vals.get("EMAILBOT_ENABLE_OCR"))),
        parse_mode="Markdown",
    )


def register_profile_handlers(app) -> None:
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CallbackQueryHandler(cb_set_profile, pattern=r"^profile:set:"))
    app.add_handler(CallbackQueryHandler(cb_toggle_ocr, pattern=r"^profile:toggle_ocr$"))
    app.add_handler(CallbackQueryHandler(cb_reset, pattern=r"^profile:reset$"))


__all__ = ["register_profile_handlers"]
