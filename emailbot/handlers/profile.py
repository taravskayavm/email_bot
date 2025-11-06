from aiogram import Router, types, F
from aiogram.utils.keyboard import InlineKeyboardBuilder

from emailbot import config
from emailbot.runtime_config import clear, set_many

router = Router()

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


def _kb(current_ocr: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="🚀 Быстрый", callback_data="profile:set:fast")
    kb.button(text="⚖️ Универсальный", callback_data="profile:set:universal")
    kb.button(text="🧱 Тяжёлый", callback_data="profile:set:heavy")
    kb.button(text=("🧠 OCR: Вкл" if current_ocr else "🧠 OCR: Выкл"), callback_data="profile:toggle_ocr")
    kb.button(text="♻️ Сбросить (по умолчанию)", callback_data="profile:reset")
    kb.adjust(3, 2)
    return kb.as_markup()


def _current_values() -> dict[str, object]:
    return {key: getattr(config, key) for key in PROFILE_KEYS}


def _render() -> tuple[str, types.InlineKeyboardMarkup]:
    values = _current_values()
    ocr_enabled = bool(values.get("EMAILBOT_ENABLE_OCR"))
    text = (
        "⚙️ Профиль парсинга PDF\n"
        f"- Адаптивный таймаут: {values.get('PDF_ADAPTIVE_TIMEOUT')}\n"
        f"- База: {values.get('PDF_TIMEOUT_BASE')} c\n"
        f"- + за 1 МБ: {values.get('PDF_TIMEOUT_PER_MB')} c/МБ\n"
        f"- Диапазон: {values.get('PDF_TIMEOUT_MIN')}–{values.get('PDF_TIMEOUT_MAX')} c\n"
        f"- PDF_MAX_PAGES: {values.get('PDF_MAX_PAGES')}\n"
        f"- OCR: {'включён' if ocr_enabled else 'выключен'}\n\n"
        "Выберите профиль или переключите OCR:"
    )
    return text, _kb(ocr_enabled)


@router.message(F.text == "/profile")
async def cmd_profile(message: types.Message):
    text, markup = _render()
    await message.answer(text, reply_markup=markup)


@router.callback_query(F.data.startswith("profile:set:"))
async def cb_set_profile(call: types.CallbackQuery):
    profile = call.data.split(":")[-1]
    cfg = PROFILES.get(profile)
    if not cfg:
        await call.answer("Неизвестный профиль", show_alert=True)
        return
    set_many(cfg)
    text, markup = _render()
    await call.message.edit_text(
        f"✅ Профиль «{profile}» применён.\n\n{text}", reply_markup=markup
    )
    await call.answer()


@router.callback_query(F.data == "profile:toggle_ocr")
async def cb_toggle_ocr(call: types.CallbackQuery):
    current = bool(getattr(config, "EMAILBOT_ENABLE_OCR"))
    set_many({"EMAILBOT_ENABLE_OCR": (not current)})
    text, markup = _render()
    await call.message.edit_text(text, reply_markup=markup)
    await call.answer("OCR переключён.")


@router.callback_query(F.data == "profile:reset")
async def cb_reset(call: types.CallbackQuery):
    clear(list(PROFILE_KEYS))
    text, markup = _render()
    await call.message.edit_text(
        "♻️ Профиль сброшен к значениям по умолчанию (.env).\n\n" + text,
        reply_markup=markup,
    )
    await call.answer()
