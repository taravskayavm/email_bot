"""Telegram bot handlers."""

from __future__ import annotations

# isort:skip_file
import asyncio
import csv
import imaplib
import json
import logging
import os
import random
import re
import secrets
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import ContextTypes

from bot.keyboards import build_templates_kb
from services.templates import get_template

from utils.email_clean import (
    canonicalize_email,
    dedupe_keep_original,
    drop_leading_char_twins,
    parse_emails_unified,
)
from utils.send_stats import summarize_today, summarize_week, current_tz_label
from utils.send_stats import _stats_path  # только для отображения пути
from utils.bounce import sync_bounces

STATS_PATH = str(_stats_path())

from . import extraction as _extraction
from . import extraction_url as _extraction_url
from . import mass_state, messaging
from . import messaging_utils as mu
from . import settings
from .extraction import normalize_email, smart_extract_emails
from .reporting import build_mass_report_text, log_mass_filter_digest
from .settings_store import DEFAULTS


# --- Новое состояние для ручного ввода исправлений ---
EDIT_SUSPECTS_INPUT = 9301


def _preclean_text_for_emails(text: str) -> str:
    return text


def apply_numeric_truncation_removal(allowed):
    return allowed, []


async def async_extract_emails_from_url(
    url: str, session, chat_id=None, batch_id: str | None = None
):
    text = await asyncio.to_thread(_extraction_url.fetch_url, url)
    _ = _extraction.extract_any  # keep reference for tests
    found = parse_emails_unified(text or " ")
    cleaned = dedupe_keep_original(found)
    cleaned = drop_leading_char_twins(cleaned)
    emails = set(cleaned)
    foreign = {e for e in emails if not is_allowed_tld(e)}
    stats: dict = {}
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": url, "count": len(emails)},
    )
    return url, emails, foreign, [], stats


def collapse_footnote_variants(emails):
    return emails


def collect_repairs_from_files(files):
    return []


async def extract_emails_from_zip(path: str, *_, **__):
    emails, stats = await asyncio.to_thread(_extraction.extract_any, path)
    emails = set(e.lower().strip() for e in emails)
    extracted_files = [path]
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(emails)},
    )
    return emails, extracted_files, set(emails), stats


def extract_emails_loose(text):
    return set(smart_extract_emails(text))


def extract_from_uploaded_file(path: str):
    emails, stats = _extraction.extract_any(path)
    emails = set(e.lower().strip() for e in emails)
    logger.info(
        "extraction complete",
        extra={"event": "extract", "source": path, "count": len(emails)},
    )
    return emails, set(emails), stats


def is_allowed_tld(email_addr: str) -> bool:
    return mu.classify_tld(email_addr) != "foreign"


def sample_preview(items, k: int):
    lst = list(dict.fromkeys(items))
    if len(lst) <= k:
        return lst
    rng = random.SystemRandom()    # не влияет на глобальное состояние
    return rng.sample(lst, k)


from .messaging import (  # noqa: E402,F401  # isort: skip
    DOWNLOAD_DIR,
    LOG_FILE,
    MAX_EMAILS_PER_DAY,
    add_blocked_email,
    clear_recent_sent_cache,
    count_sent_today,
    dedupe_blocked_file,
    get_blocked_emails,
    get_preferred_sent_folder,
    get_sent_today,
    log_sent_email,
    send_email_with_sessions,
    sync_log_with_imap,
    was_emailed_recently,
)
from .messaging_utils import (  # noqa: E402  # isort: skip
    BOUNCE_LOG_PATH,
    add_bounce,
    is_foreign,
    is_hard_bounce,
    is_soft_bounce,
    is_suppressed,
    suppress_add,
)
from .utils import log_error  # noqa: E402
from utils.smtp_client import RobustSMTP  # noqa: E402

from . import history_service

from emailbot.handlers import (
    start,
    manual_mode,
    select_group,
    proceed_to_group,
    send_all,
    preview_go_back,
)

logger = logging.getLogger(__name__)

ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

PREVIEW_ALLOWED = int(os.getenv("EXAMPLES_COUNT", "10"))
PREVIEW_FOREIGN = 5

TECH_PATTERNS = [
    "noreply",
    "no-reply",
    "do-not-reply",
    "donotreply",
    "postmaster",
    "mailer-daemon",
    "abuse",
    "support",
    "admin",
    "info@",
]


@dataclass
class SessionState:
    all_emails: Set[str] = field(default_factory=set)
    all_files: List[str] = field(default_factory=list)
    to_send: List[str] = field(default_factory=list)
    foreign: List[str] = field(default_factory=list)
    preview_allowed_all: List[str] = field(default_factory=list)
    dropped: List[Tuple[str, str]] = field(default_factory=list)
    repairs: List[tuple[str, str]] = field(default_factory=list)
    repairs_sample: List[str] = field(default_factory=list)
    group: Optional[str] = None  # template code
    template: Optional[str] = None
    template_label: Optional[str] = None
    footnote_dupes: int = 0


def _normalize_template_code(code: str) -> str:
    return (code or "").strip().lower()


def _template_label(info) -> str:
    if not info:
        return ""
    label = info.get("label") if isinstance(info, dict) else ""
    if not label:
        label = info.get("code") if isinstance(info, dict) else ""
    return str(label or "")


def _template_path(info) -> Path | None:
    if not info or not isinstance(info, dict):
        return None
    path = info.get("path")
    if not path:
        return None
    try:
        return Path(path)
    except Exception:
        return None


FORCE_SEND_CHAT_IDS: set[int] = set()
SESSION_KEY = "state"


# === Конфиг для ручной рассылки (правило 180 дней) ===
def _manual_cfg():
    import os

    enforce = os.getenv("MANUAL_ENFORCE_180", "1") == "1"
    default_days = history_service.get_days_rule_default()
    try:
        days = int(os.getenv("MANUAL_DAYS", str(default_days)))
    except Exception:
        days = default_days
    allow_override = os.getenv("MANUAL_ALLOW_OVERRIDE", "1") == "1"
    return enforce, days, allow_override


def _filter_by_180(
    emails: list[str], group: str, days: int
) -> tuple[list[str], list[str]]:
    """Разделяет список на разрешённые и отклонённые по правилу N дней."""

    try:
        return history_service.filter_by_days(emails, group, days)
    except Exception:  # pragma: no cover - defensive fallback
        # в случае ошибки проверки — перестрахуемся и разрешим
        return list(emails), []


def init_state(context: ContextTypes.DEFAULT_TYPE) -> SessionState:
    """Initialize session state for the current chat."""
    state = SessionState()
    context.chat_data[SESSION_KEY] = state
    context.chat_data["cancel_event"] = asyncio.Event()
    return state


def get_state(context: ContextTypes.DEFAULT_TYPE) -> SessionState:
    """Return existing session state or initialize a new one."""
    return context.chat_data.get(SESSION_KEY) or init_state(context)


def enable_force_send(chat_id: int) -> None:
    """Allow this chat to bypass the daily sending limit."""

    FORCE_SEND_CHAT_IDS.add(chat_id)


def disable_force_send(chat_id: int) -> None:
    """Disable the force-send mode for the chat."""

    FORCE_SEND_CHAT_IDS.discard(chat_id)


def is_force_send(chat_id: int) -> bool:
    """Return ``True`` if the chat bypasses the daily limit."""

    return chat_id in FORCE_SEND_CHAT_IDS


def clear_all_awaiting(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reset all awaiting flags stored in ``user_data``."""

    for key in ["awaiting_block_email", "awaiting_manual_email"]:
        context.user_data[key] = False


async def features(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to toggle experimental features."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("Команда доступна только администратору.")
        return

    settings.load()

    def _status() -> str:
        lines = []
        line = f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}"
        if settings.STRICT_OBFUSCATION == DEFAULTS["STRICT_OBFUSCATION"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}"
        if settings.FOOTNOTE_RADIUS_PAGES == DEFAULTS["FOOTNOTE_RADIUS_PAGES"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}"
        if settings.PDF_LAYOUT_AWARE == DEFAULTS["PDF_LAYOUT_AWARE"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        if settings.ENABLE_OCR == DEFAULTS["ENABLE_OCR"]:
            line += " (рекомендуется)"
        lines.append(line)
        return "\n".join(lines)

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'Строгий' if settings.STRICT_OBFUSCATION else 'Обычный'} ⏼",
                        callback_data="feat:strict:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сноски: радиус 0", callback_data="feat:radius:0"
                    ),
                    InlineKeyboardButton("1", callback_data="feat:radius:1"),
                    InlineKeyboardButton("2", callback_data="feat:radius:2"),
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feat:layout:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feat:ocr:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сбросить к рекомендованным",
                        callback_data="feat:reset:defaults",
                    )
                ],
            ]
        )

    def _doc() -> str:
        return (
            "ℹ️ Рекомендуемые настройки: строгие обфускации — ON, радиус сносок — 1, "
            "PDF-layout — OFF, OCR — OFF."
        )

    await update.message.reply_text(
        f"{_status()}\n\n{_doc()}", reply_markup=_keyboard()
    )


async def features_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle feature toggle button callbacks."""

    query = update.callback_query
    if not query:
        return
    user = query.from_user
    if not user or user.id not in ADMIN_IDS:
        await query.answer()
        return

    settings.load()

    data = query.data or ""
    hint = ""
    try:
        if data == "feat:strict:toggle":
            settings.STRICT_OBFUSCATION = not settings.STRICT_OBFUSCATION
            hint = (
                "🛡️ Строгий режим включён. Парсер принимает обфускации только с явными “at/dot”. "
                "Ложные «121536@gmail.com» с чисел не появятся. На реальные адреса с @/mailto это не влияет."
                if settings.STRICT_OBFUSCATION
                else "⚠️ Строгий режим выключен. Парсер будет пытаться восстановить адреса из менее явных обфускаций. Возможен рост ложных совпадений на «число + домен»."
            )
        elif data.startswith("feat:radius:"):
            n = int(data.rsplit(":", 1)[-1])
            if n not in {0, 1, 2}:
                raise ValueError
            settings.FOOTNOTE_RADIUS_PAGES = n
            hint = f"📝 Радиус сносок: {n}. Дубликаты «урезанных» адресов будут склеиваться в пределах той же страницы и ±{n} стр. того же файла."
        elif data == "feat:layout:toggle":
            settings.PDF_LAYOUT_AWARE = not settings.PDF_LAYOUT_AWARE
            hint = (
                "📄 Учёт макета PDF включён. Надстрочные (сноски) обрабатываются точнее. Может работать медленнее на больших PDF."
                if settings.PDF_LAYOUT_AWARE
                else "📄 Учёт макета PDF выключен. Используется стандартное извлечение текста."
            )
        elif data == "feat:ocr:toggle":
            settings.ENABLE_OCR = not settings.ENABLE_OCR
            hint = (
                "🔍 OCR включён. Будем распознавать e-mail в скан-PDF. Анализ станет медленнее. Ограничения: до 10 страниц, таймаут 30 сек."
                if settings.ENABLE_OCR
                else "🔍 OCR выключен. Скан-PDF без текста пропускаются без распознавания."
            )
        elif data == "feat:reset:defaults":
            settings.STRICT_OBFUSCATION = DEFAULTS["STRICT_OBFUSCATION"]
            settings.FOOTNOTE_RADIUS_PAGES = DEFAULTS["FOOTNOTE_RADIUS_PAGES"]
            settings.PDF_LAYOUT_AWARE = DEFAULTS["PDF_LAYOUT_AWARE"]
            settings.ENABLE_OCR = DEFAULTS["ENABLE_OCR"]
            hint = "↩️ Сброшено к рекомендованным настройкам."
        else:
            raise ValueError
        settings.save()
    except Exception:
        hint = "⛔ Недопустимое значение."

    def _status() -> str:
        lines = []
        line = f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}"
        if settings.STRICT_OBFUSCATION == DEFAULTS["STRICT_OBFUSCATION"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}"
        if settings.FOOTNOTE_RADIUS_PAGES == DEFAULTS["FOOTNOTE_RADIUS_PAGES"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}"
        if settings.PDF_LAYOUT_AWARE == DEFAULTS["PDF_LAYOUT_AWARE"]:
            line += " (рекомендуется)"
        lines.append(line)
        line = f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        if settings.ENABLE_OCR == DEFAULTS["ENABLE_OCR"]:
            line += " (рекомендуется)"
        lines.append(line)
        return "\n".join(lines)

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'Строгий' if settings.STRICT_OBFUSCATION else 'Обычный'} ⏼",
                        callback_data="feat:strict:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сноски: радиус 0", callback_data="feat:radius:0"
                    ),
                    InlineKeyboardButton("1", callback_data="feat:radius:1"),
                    InlineKeyboardButton("2", callback_data="feat:radius:2"),
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feat:layout:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feat:ocr:toggle",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сбросить к рекомендованным",
                        callback_data="feat:reset:defaults",
                    )
                ],
            ]
        )

    def _doc() -> str:
        return (
            "ℹ️ Рекомендуемые настройки: строгие обфускации — ON, радиус сносок — 1, "
            "PDF-layout — OFF, OCR — OFF."
        )

    await query.answer()
    await query.edit_message_text(
        f"{_status()}\n\n{hint}\n\n{_doc()}", reply_markup=_keyboard()
    )


async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin diagnostic command with runtime information."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return

    import csv
    import sys
    from datetime import datetime

    import aiohttp
    import telegram

    from .messaging_utils import BOUNCE_LOG_PATH

    versions = {
        "python": sys.version.split()[0],
        "telegram": telegram.__version__,
        "aiohttp": aiohttp.__version__,
    }
    bounce_today = 0
    if BOUNCE_LOG_PATH.exists():
        today = datetime.utcnow().date()
        with BOUNCE_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    dt = datetime.fromisoformat(row.get("ts", ""))
                    if dt.date() == today:
                        bounce_today += 1
                except Exception:
                    pass

    flags = {
        "STRICT_OBFUSCATION": settings.STRICT_OBFUSCATION,
        "PDF_LAYOUT_AWARE": settings.PDF_LAYOUT_AWARE,
        "ENABLE_OCR": settings.ENABLE_OCR,
    }

    lines = [
        "Versions:",
        f"  Python: {versions['python']}",
        f"  telegram: {versions['telegram']}",
        f"  aiohttp: {versions['aiohttp']}",
        "Limits:",
        f"  MAX_EMAILS_PER_DAY: {MAX_EMAILS_PER_DAY}",
        "Flags:",
    ]
    for k, v in flags.items():
        lines.append(f"  {k}: {v}")
    lines.extend(
        [
            "Counters:",
            f"  sent_today: {count_sent_today()}",
            f"  bounces_today: {bounce_today}",
        ]
    )
    await update.message.reply_text("\n".join(lines))


async def dedupe_log_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Admin command to deduplicate sent log."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return
    if context.args and context.args[0].lower() in {"yes", "y"}:
        result = mu.dedupe_sent_log_inplace(messaging.LOG_FILE)
        await update.message.reply_text(str(result))
    else:
        await update.message.reply_text(
            "⚠️ Это действие перезапишет sent_log.csv. Запустите /dedupe_log yes для подтверждения."
        )


async def prompt_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to upload files or URLs with e-mail addresses."""

    await update.message.reply_text(
        (
            "📥 Загрузите данные с e-mail-адресами для рассылки.\n\n"
            "Поддерживаемые форматы: PDF, Excel (.xlsx), Word (.docx), CSV, "
            "ZIP (с этими файлами внутри), а также ссылки на сайты."
        )
    )


async def about_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a short description of the bot."""

    await update.message.reply_text(
        (
            "Бот делает рассылку HTML-писем с учётом истории отправки "
            "(IMAP 180 дней) и блок-листа. Один адрес — не чаще 1 раза в 6 "
            "месяцев. Домены: только .ru и .com."
        )
    )


async def stop_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the stop button by signalling cancellation."""
    event = context.chat_data.get("cancel_event")
    if event:
        event.set()
    await update.message.reply_text("Остановлено…")
    context.chat_data["cancel_event"] = asyncio.Event()


async def add_block_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ask the user to provide e-mails to add to the block list."""

    clear_all_awaiting(context)
    await update.message.reply_text(
        (
            "Введите email или список email-адресов "
            "(через запятую/пробел/с новой строки), "
            "которые нужно добавить в исключения:"
        )
    )
    context.user_data["awaiting_block_email"] = True


async def show_blocked_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the current list of blocked e-mail addresses."""

    dedupe_blocked_file()
    blocked = get_blocked_emails()
    if not blocked:
        await update.message.reply_text("📄 Список исключений пуст.")
    else:
        await update.message.reply_text(
            "📄 В исключениях:\n" + "\n".join(sorted(blocked))
        )


async def prompt_change_group(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Prompt the user to choose a mailing group."""

    await update.message.reply_text(
        "Выберите направление:",
        reply_markup=build_templates_kb(
            context.chat_data.get("current_template_code")
        ),
    )


async def imap_folders_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """List available IMAP folders and allow user to choose."""

    try:
        imap = imaplib.IMAP4_SSL("imap.mail.ru")
        imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
        status, data = imap.list()
        imap.logout()
        if status != "OK" or not data:
            await update.message.reply_text("❌ Не удалось получить список папок.")
            return
        folders = [
            line.decode(errors="ignore").split(' "', 2)[-1].strip('"') for line in data
        ]
        context.user_data["imap_folders"] = folders
        await _show_imap_page(update, context, 0)
    except Exception as e:
        log_error(f"imap_folders_command: {e}")
        await update.message.reply_text(f"❌ Ошибка IMAP: {e}")


async def _show_imap_page(update_or_query, context, page: int) -> None:
    folders = context.user_data.get("imap_folders", [])
    per_page = 6
    start = page * per_page
    sub = folders[start : start + per_page]
    keyboard = [
        [
            InlineKeyboardButton(
                f,
                callback_data="imap_choose:" + urllib.parse.quote(f, safe=""),
            )
        ]
        for f in sub
    ]
    if len(folders) > per_page:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"imap_page:{page - 1}"))
        if start + per_page < len(folders):
            nav.append(InlineKeyboardButton("➡️", callback_data=f"imap_page:{page + 1}"))
        keyboard.append(nav)
    markup = InlineKeyboardMarkup(keyboard)
    text = "Выберите папку для сохранения отправленных писем:"
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(text, reply_markup=markup)
    else:
        await update_or_query.message.edit_text(text, reply_markup=markup)


async def imap_page_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(":")[1])
    await _show_imap_page(query, context, page)


async def choose_imap_folder(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    await query.answer()
    encoded = query.data.split(":", 1)[1]
    folder = urllib.parse.unquote(encoded)
    with open(messaging.IMAP_FOLDER_FILE, "w", encoding="utf-8") as f:
        f.write(folder)
    await query.message.reply_text(f"📁 Папка сохранена: {folder}")


async def force_send_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Enable ignoring of the daily sending limit for this chat."""

    chat_id = update.effective_chat.id
    enable_force_send(chat_id)
    await update.message.reply_text(
        "Режим игнорирования дневного лимита включён для этого чата.\n"
        "Запустите рассылку ещё раз — ограничение на сегодня будет проигнорировано."
    )


async def handle_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отчёт об отправках за сегодня и неделю."""

    today = summarize_today()
    week = summarize_week()
    tz = current_tz_label()
    lines = [
        f"📝 Отчёт ({tz}):",
        f"Сегодня — ок: {today.get('ok',0)}, ошибок: {today.get('err',0)}",
        f"Неделя — ок: {week.get('ok',0)}, ошибок: {week.get('err',0)}",
    ]
    await update.message.reply_text("\n".join(lines))


async def handle_reports_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Диагностика отчётов: путь, наличие, хвост и текущее время."""

    try:
        p = Path(STATS_PATH)
        exists = p.exists()
        tail: list[str] = []
        if exists:
            with p.open("r", encoding="utf-8") as f:
                lines = f.readlines()[-5:]
                tail = [l.strip() for l in lines]
        now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        msg = [
            "🛠 Диагностика отчётов:",
            f"Файл: {p}",
            f"Существует: {exists}",
            f"Последние записи ({len(tail)}):",
            *tail,
            "",
            f"Время сейчас (UTC): {now_utc}",
            f"TZ отчёта: {current_tz_label()}",
        ]
        await update.message.reply_text("\n".join(msg))
    except Exception as e:  # pragma: no cover - best effort
        await update.message.reply_text(f"Diag error: {e!r}")


# === КНОПКИ ДЛЯ ПОДОЗРИТЕЛЬНЫХ ===
async def on_accept_suspects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    suspects = context.user_data.get("emails_suspects") or []
    if not suspects:
        return await q.edit_message_text("Подозрительных адресов нет.")
    sendable = set(context.user_data.get("emails_for_sending") or [])
    for e in suspects:
        sendable.add(e)
    context.user_data["emails_for_sending"] = sorted(sendable)
    context.user_data["emails_suspects"] = []
    await q.edit_message_text(
        "✅ Подозрительные адреса приняты и добавлены к отправке.\n"
        f"Итого к отправке: {len(sendable)}"
    )


async def on_edit_suspects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    suspects = context.user_data.get("emails_suspects") or []
    preview = "\n".join(suspects[:10]) if suspects else "—"
    await q.edit_message_text(
        "✍️ Введите исправленные e-mail одним блоком (через пробел/запятую/с новой строки).\n"
        "Текущие «подозрительные» (первые 10):\n" + preview
    )
    context.user_data["await_edit_suspects"] = True
    return EDIT_SUSPECTS_INPUT


async def on_edit_suspects_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("await_edit_suspects"):
        return
    text = update.message.text or ""
    fixed = parse_emails_unified(text)
    fixed = dedupe_keep_original(fixed)
    fixed = drop_leading_char_twins(fixed)
    sendable = set(context.user_data.get("emails_for_sending") or [])
    for e in fixed:
        sendable.add(e)
    context.user_data["emails_for_sending"] = sorted(sendable)
    context.user_data["emails_suspects"] = []
    context.user_data["await_edit_suspects"] = False
    await update.message.reply_text(
        "✅ Исправленные адреса приняты.\n"
        f"Итого к отправке: {len(sendable)}"
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to select a reporting period."""

    keyboard = [
        [InlineKeyboardButton("📆 День", callback_data="report_day")],
        [InlineKeyboardButton("🗓 Неделя", callback_data="report_week")],
        [InlineKeyboardButton("🗓 Месяц", callback_data="report_month")],
        [InlineKeyboardButton("📅 Год", callback_data="report_year")],
    ]
    await update.message.reply_text(
        "Выберите период отчёта:", reply_markup=InlineKeyboardMarkup(keyboard)
    )


def get_report(period: str = "day") -> str:
    """Return statistics of sent e-mails for the given period."""
    if period == "day":
        s = summarize_today()
        return f"Успешных: {s.get('ok',0)}\nОшибок: {s.get('err',0)}"
    if period == "week":
        s = summarize_week()
        return f"Успешных: {s.get('ok',0)}\nОшибок: {s.get('err',0)}"

    if not os.path.exists(LOG_FILE):
        return "Нет данных о рассылках."
    now = datetime.now()
    if period == "month":
        start_at = now - timedelta(days=30)
    elif period == "year":
        start_at = now - timedelta(days=365)
    else:
        start_at = now - timedelta(days=1)

    cnt_ok = 0
    cnt_err = 0
    with open(LOG_FILE, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 4:
                continue
            try:
                dt = datetime.fromisoformat(row[0])
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
            except Exception:
                continue
            if dt >= start_at:
                st = (row[3] or "").strip().lower()
                if st == "ok":
                    cnt_ok += 1
                else:
                    cnt_err += 1
    return f"Успешных: {cnt_ok}\nОшибок: {cnt_err}"


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the selected report to the user."""

    query = update.callback_query
    await query.answer()
    period = query.data.replace("report_", "")
    mapping = {
        "day": "Отчёт за день",
        "week": "Отчёт за неделю",
        "month": "Отчёт за месяц",
        "year": "Отчёт за год",
    }
    text = get_report(period)
    header = mapping.get(period, period)
    if period in ("day", "week"):
        header = f"{header} ({current_tz_label()})"
    await query.edit_message_text(f"📊 {header}:\n{text}")


async def sync_imap_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Synchronize local log with the IMAP "Sent" folder."""

    await update.message.reply_text(
        "⏳ Сканируем папку «Отправленные» (последние 180 дней)..."
    )
    try:
        stats = sync_log_with_imap()
        clear_recent_sent_cache()
        await update.message.reply_text(
            "🔄 "
            f"новых: {stats['new_contacts']}, обновлено: {stats['updated_contacts']}, "
            f"пропущено: {stats['skipped_events']}, всего: {stats['total_rows_after']}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка синхронизации: {e}")


async def sync_bounces_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check INBOX for bounce messages and log them."""
    await update.message.reply_text("⏳ Проверяю INBOX на бонсы...")
    try:
        n = sync_bounces()
        await update.message.reply_text(
            f"✅ Найдено и добавлено в отчёты: {n} bounce-сообщений."
        )
    except Exception as e:  # pragma: no cover - best effort
        await update.message.reply_text(f"❌ Ошибка при синхронизации бонсов: {e}")


async def retry_last_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Retry sending e-mails that previously soft-bounced."""

    rows: list[dict] = []
    if BOUNCE_LOG_PATH.exists():
        with BOUNCE_LOG_PATH.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    if not rows:
        await update.message.reply_text("Нет писем для ретрая")
        return
    last_ts = rows[-1]["ts"]
    addrs: list[str] = []
    for r in reversed(rows):
        if r["ts"] != last_ts:
            break
        code = r.get("code") or None
        try:
            icode = int(code) if code else None
        except Exception:
            icode = None
        if is_soft_bounce(icode, r.get("msg")):
            email = (r.get("email") or "").lower().strip()
            if email:
                addrs.append(email)
    unique = list(dict.fromkeys(addrs))
    if not unique:
        await update.message.reply_text("Нет писем для ретрая")
        return
    sent = 0
    for addr in unique:
        if is_suppressed(addr):
            continue
        try:
            messaging.send_raw_smtp_with_retry("retry", addr)
            log_sent_email(addr, "retry")
            sent += 1
        except Exception as e:
            logger.warning("retry_last send failed for %s: %s", addr, e)
    await update.message.reply_text(f"Повторно отправлено: {sent}")


async def reset_email_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear stored e-mails and reset the session state."""

    chat_id = update.effective_chat.id
    init_state(context)
    context.chat_data.pop("manual_all_emails", None)
    context.chat_data.pop("send_preview", None)
    context.chat_data.pop("fix_pending", None)
    context.chat_data["batch_id"] = None
    mass_state.clear_batch(chat_id)
    context.chat_data["extract_lock"] = asyncio.Lock()
    await update.message.reply_text(
        "Список email-адресов и файлов очищен. Можно загружать новые файлы!"
    )


async def _compose_report_and_save(
    context: ContextTypes.DEFAULT_TYPE,
    allowed_all: Set[str],
    filtered: List[str],
    dropped: List[Tuple[str, str]],
    foreign: List[str],
    footnote_dupes: int = 0,
) -> str:
    """Compose a summary report and store samples in session state."""

    state = get_state(context)
    state.preview_allowed_all = sorted(filtered)
    state.dropped = list(dropped)
    state.foreign = sorted(foreign)
    state.footnote_dupes = footnote_dupes

    context.chat_data["send_preview"] = {
        "final": list(dict.fromkeys(state.preview_allowed_all)),
        "dropped": list(dropped),
        "fixed": [],
    }
    context.chat_data.pop("fix_pending", None)

    sample_allowed = sample_preview(state.preview_allowed_all, PREVIEW_ALLOWED)
    sample_foreign = sample_preview(state.foreign, PREVIEW_FOREIGN)

    report_lines = [
        "✅ Анализ завершён.",
        f"Найдено адресов: {len(allowed_all)}",
        f"📧 К отправке: {len(filtered)} адресов",
        f"⚠️ Подозрительные: {len(dropped)} адресов",
        f"🌍 Иностранные домены: {len(foreign)}",
    ]
    report = "\n".join(report_lines)
    if footnote_dupes:
        report += f"\nВозможные сносочные дубликаты удалены: {footnote_dupes}"
    if sample_allowed:
        report += "\n\n🧪 Примеры:\n" + "\n".join(sample_allowed)
    if dropped:
        preview_lines = [
            "\n⚠️ Подозрительные адреса:",
            *(
                f"{i + 1}) {addr} — {reason}"
                for i, (addr, reason) in enumerate(dropped[:10])
            ),
        ]
        report += "\n" + "\n".join(preview_lines)
        report += "\nНажмите «✏️ Исправить №…» чтобы отредактировать."
    if sample_foreign:
        report += "\n\n🌍 Примеры иностранных:\n" + "\n".join(sample_foreign)
    return report


async def request_fix(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to provide a fixed e-mail address."""

    query = update.callback_query
    await query.answer()
    preview = context.chat_data.get("send_preview", {})
    dropped = preview.get("dropped", [])
    data = query.data or ""
    try:
        _, idx_s = data.split(":", 1)
        idx = int(idx_s)
    except Exception:
        await query.message.reply_text("⚠️ Некорректный индекс.")
        return
    if idx < 0 or idx >= len(dropped):
        await query.message.reply_text("⚠️ Индекс вне диапазона.")
        return
    original, reason = dropped[idx]
    context.chat_data["fix_pending"] = {"index": idx, "original": original}
    await query.message.reply_text(
        (
            "Введите исправленный адрес для:\n"
            f"`{original}`\n(прежняя причина: {reason})"
        ),
        parse_mode="Markdown",
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle an uploaded document with potential e-mail addresses."""

    doc = update.message.document
    if not doc:
        return
    chat_id = update.effective_chat.id
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(
        DOWNLOAD_DIR, f"{chat_id}_{int(time.time())}_{doc.file_name}"
    )
    f = await doc.get_file()
    await f.download_to_drive(file_path)

    await update.message.reply_text("Файл загружен. Идёт анализ...")
    _progress_msg = await update.message.reply_text("🔎 Анализируем...")

    allowed_all, loose_all = set(), set()
    extracted_files: List[str] = []
    repairs: List[tuple[str, str]] = []
    footnote_dupes = 0

    try:
        if file_path.lower().endswith(".zip"):
            allowed, extracted_files, loose, stats = await extract_emails_from_zip(
                file_path
            )
            allowed_all.update(allowed)
            loose_all.update(loose)
            repairs = collect_repairs_from_files(extracted_files)
            footnote_dupes += stats.get("footnote_pairs_merged", 0)
        else:
            allowed, loose, stats = extract_from_uploaded_file(file_path)
            allowed_all.update(allowed)
            loose_all.update(loose)
            extracted_files.append(file_path)
            repairs = collect_repairs_from_files([file_path])
            footnote_dupes += stats.get("footnote_pairs_merged", 0)
    except Exception as e:
        log_error(f"handle_document: {file_path}: {e}")

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(repairs + trunc_pairs))

    technical_emails = [e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)]
    filtered = [
        e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
    ]

    dropped_current: List[Tuple[str, str]] = []
    for email in sorted(allowed_all):
        if email in filtered:
            continue
        if email in technical_emails:
            dropped_current.append((email, "technical-address"))
        elif not is_allowed_tld(email):
            dropped_current.append((email, "foreign-domain"))
        else:
            dropped_current.append((email, "filtered"))

    foreign_raw = {e for e in loose_all if not is_allowed_tld(e)}
    foreign = sorted(collapse_footnote_variants(foreign_raw))

    state = get_state(context)
    state.all_emails.update(allowed_all)
    state.all_files.extend(extracted_files)
    current = set(state.to_send)
    current.update(filtered)
    state.to_send = sorted(current)
    state.repairs = list(dict.fromkeys((state.repairs or []) + repairs))
    state.repairs_sample = sample_preview([f"{b} → {g}" for (b, g) in state.repairs], 6)
    all_allowed = state.all_emails
    foreign_total = set(state.foreign) | set(foreign)
    total_footnote = state.footnote_dupes + footnote_dupes

    existing = list(state.dropped or [])
    combined_map: dict[str, str] = {}
    for addr, reason in existing + dropped_current:
        if addr not in combined_map:
            combined_map[addr] = reason
    dropped_total = [(addr, combined_map[addr]) for addr in combined_map]

    report = await _compose_report_and_save(
        context,
        all_allowed,
        state.to_send,
        dropped_total,
        sorted(foreign_total),
        total_footnote,
    )
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for s in state.repairs_sample:
            report += f"\n{s}"
    preview = context.chat_data.get("send_preview", {})
    dropped_preview = preview.get("dropped", [])
    fix_buttons: List[InlineKeyboardButton] = []
    for idx in range(min(len(dropped_preview), 5)):
        fix_buttons.append(
            InlineKeyboardButton(
                f"✏️ Исправить №{idx + 1}", callback_data=f"fix:{idx}"
            )
        )

    extra_buttons: List[List[InlineKeyboardButton]] = []
    if fix_buttons:
        extra_buttons.append(fix_buttons)
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "🔁 Показать ещё примеры", callback_data="refresh_preview"
            )
        ]
    )
    if state.repairs:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    f"🧩 Применить исправления ({len(state.repairs)})",
                    callback_data="apply_repairs",
                )
            ]
        )
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "🧩 Показать все исправления", callback_data="show_repairs"
                )
            ]
        )
    extra_buttons.append(
        [
            InlineKeyboardButton(
                "▶️ Перейти к выбору направления", callback_data="proceed_group"
            )
        ]
    )
    report += "\n\nДополнительные действия:"
    await update.message.reply_text(
        report,
        reply_markup=InlineKeyboardMarkup(extra_buttons),
    )


async def refresh_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a fresh sample of extracted e-mail addresses."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    allowed_all = state.preview_allowed_all if state else []
    dropped = state.dropped if state else []
    foreign = state.foreign if state else []
    if not (allowed_all or dropped or foreign):
        await query.answer(
            "Нет данных для примеров. Загрузите файл/ссылки.", show_alert=True
        )
        return
    await query.answer()
    sample_allowed = sample_preview(allowed_all, PREVIEW_ALLOWED)
    sample_foreign = sample_preview(foreign, PREVIEW_FOREIGN)
    report = []
    if sample_allowed:
        report.append("🧪 Примеры:\n" + "\n".join(sample_allowed))
    if dropped:
        preview_lines = [
            f"{i + 1}) {addr} — {reason}"
            for i, (addr, reason) in enumerate(dropped[:5])
        ]
        if preview_lines:
            report.append("⚠️ Подозрительные:\n" + "\n".join(preview_lines))
    if sample_foreign:
        report.append("🌍 Примеры иностранных:\n" + "\n".join(sample_foreign))
    await query.message.reply_text(
        "\n\n".join(report) if report else "Показать нечего."
    )


async def prompt_manual_email(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to enter e-mail addresses manually."""

    clear_all_awaiting(context)
    context.chat_data.pop("manual_all_emails", None)
    context.chat_data.pop("manual_send_mode", None)
    context.chat_data.pop("manual_allowed_preview", None)
    context.chat_data.pop("manual_rejected_preview", None)
    context.chat_data.pop("manual_selected_template_code", None)
    context.chat_data.pop("manual_selected_template_label", None)
    context.chat_data.pop("manual_selected_emails", None)
    await update.message.reply_text(
        (
            "Введите email или список email-адресов "
            "(через запятую/пробел/с новой строки):"
        )
    )
    context.user_data["awaiting_manual_email"] = True


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process text messages for uploads, blocking or manual lists."""

    chat_id = update.effective_chat.id
    text = update.message.text or ""
    fix_state = context.chat_data.get("fix_pending")
    if fix_state:
        new_text = text.strip()
        if not new_text:
            await update.message.reply_text("❌ Введите корректный адрес.")
            return
        from pipelines.extract_emails import run_pipeline_on_text

        final_new, dropped_new = run_pipeline_on_text(new_text)
        if final_new and not dropped_new:
            new_email = final_new[0]
            preview = context.chat_data.get("send_preview", {}) or {}
            dropped_list = list(preview.get("dropped", []))
            idx = fix_state.get("index", -1)
            original = fix_state.get("original")
            if 0 <= idx < len(dropped_list) and dropped_list[idx][0] == original:
                dropped_list.pop(idx)
            else:
                dropped_list = [pair for pair in dropped_list if pair[0] != original]
            preview["dropped"] = dropped_list
            final_list = [
                item for item in list(preview.get("final", [])) if item != original
            ]
            final_list.append(new_email)
            preview["final"] = list(dict.fromkeys(final_list))
            fixed_list = list(preview.get("fixed", []))
            fixed_list.append({"from": original, "to": new_email})
            preview["fixed"] = fixed_list
            context.chat_data["send_preview"] = preview
            context.chat_data.pop("fix_pending", None)

            state = get_state(context)
            state.dropped = [pair for pair in state.dropped if pair[0] != original]
            state.foreign = sorted(addr for addr in state.foreign if addr != original)
            to_send_set = set(state.to_send)
            to_send_set.discard(original)
            to_send_set.add(new_email)
            state.to_send = sorted(to_send_set)
            preview_allowed = [
                addr for addr in state.preview_allowed_all if addr != original
            ]
            preview_allowed.append(new_email)
            state.preview_allowed_all = sorted(set(preview_allowed))
            await update.message.reply_text(
                f"✅ Исправлено: `{original}` → **{new_email}**",
                parse_mode="Markdown",
            )
        else:
            reason = dropped_new[0][1] if dropped_new else "invalid"
            await update.message.reply_text(
                f"❌ Всё ещё некорректно ({reason}). Попробуйте ещё раз или отправьте другой адрес."
            )
        return
    if context.user_data.get("awaiting_block_email"):
        clean = _preclean_text_for_emails(text)
        emails = {normalize_email(x) for x in extract_emails_loose(clean) if "@" in x}
        added = [e for e in emails if add_blocked_email(e)]
        await update.message.reply_text(
            f"Добавлено в исключения: {len(added)}" if added else "Ничего не добавлено."
        )
        context.user_data["awaiting_block_email"] = False
        return
    if context.user_data.get("awaiting_manual_email"):
        # Единый вход: всё — через единый пайплайн
        found_emails = parse_emails_unified(text)
        emails = dedupe_keep_original(found_emails)
        emails = drop_leading_char_twins(emails)
        emails = sorted(emails, key=str.lower)
        logger.info("Manual input parsing: raw=%r emails=%r", text, emails)
        if not emails:
            await update.message.reply_text("❌ Не найдено ни одного email.")
            return

        # Скрываем список адресов: считаем только количества
        context.user_data["awaiting_manual_email"] = False
        context.chat_data["manual_all_emails"] = emails
        context.chat_data["manual_send_mode"] = "allowed"  # allowed|all

        template_rows = [
            row[:]
            for row in build_templates_kb(
                context.chat_data.get("manual_selected_template_code"),
                prefix="manual_tpl:",
            ).inline_keyboard
        ]

        enforce, days, allow_override = _manual_cfg()
        if enforce:
            allowed, rejected = _filter_by_180(emails, group="", days=days)
        else:
            allowed, rejected = (emails, [])

        context.chat_data["manual_allowed_preview"] = allowed
        context.chat_data["manual_rejected_preview"] = rejected
        lines = ["Адреса получены.", f"К отправке (предварительно): {len(allowed)}"]
        if rejected:
            lines.append(f"Отфильтровано по правилу {days} дней: {len(rejected)}")

        mode_row = []
        if allow_override and rejected:
            mode_row = [
                InlineKeyboardButton(
                    "Отправить только разрешённым", callback_data="manual_mode_allowed"
                ),
                InlineKeyboardButton("Отправить всем", callback_data="manual_mode_all"),
            ]
        keyboard = [*template_rows]
        if mode_row:
            keyboard.append(mode_row)
        keyboard.append([InlineKeyboardButton("♻️ Сброс", callback_data="manual_reset")])

        await update.message.reply_text(
            "\n".join(lines) + "\n\n⬇️ Выберите направление письма:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    urls = re.findall(r"https?://\S+", text)
    if urls:
        lock = context.chat_data.setdefault("extract_lock", asyncio.Lock())
        if lock.locked():
            await update.message.reply_text("⏳ Уже идёт анализ этого URL")
            return
        now = time.monotonic()
        last = context.chat_data.get("last_url")
        if last and last.get("urls") == urls and now - last.get("ts", 0) < 10:
            await update.message.reply_text("⏳ Уже идёт анализ этого URL")
            return
        context.chat_data["last_url"] = {"urls": urls, "ts": now}
        batch_id = secrets.token_hex(8)
        context.chat_data["batch_id"] = batch_id
        mass_state.set_batch(chat_id, batch_id)
        _extraction_url.set_batch(batch_id)
        context.chat_data["entry_url"] = urls[0]
        await update.message.reply_text("🌐 Загружаем страницы...")
        results = []
        async with lock:
            async with aiohttp.ClientSession() as session:
                tasks = [
                    async_extract_emails_from_url(url, session, chat_id, batch_id)
                    for url in sorted(urls)
                ]
                results = await asyncio.gather(*tasks)
        if batch_id != context.chat_data.get("batch_id"):
            return
        allowed_all: Set[str] = set()
        foreign_all: Set[str] = set()
        repairs_all: List[tuple[str, str]] = []
        footnote_dupes = 0
        for _, allowed, foreign, repairs, stats in results:
            allowed_all.update(allowed)
            foreign_all.update(foreign)
            repairs_all.extend(repairs)
            footnote_dupes += stats.get("footnote_pairs_merged", 0)

        technical_emails = [
            e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)
        ]
        filtered = sorted(
            e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
        )
        dropped_current: List[Tuple[str, str]] = []
        for email in sorted(allowed_all):
            if email in filtered:
                continue
            if email in technical_emails:
                dropped_current.append((email, "technical-address"))
            elif not is_allowed_tld(email):
                dropped_current.append((email, "foreign-domain"))
            else:
                dropped_current.append((email, "filtered"))

        state = get_state(context)
        state.all_emails.update(allowed_all)
        current = set(state.to_send)
        current.update(filtered)
        state.to_send = sorted(current)
        foreign_total = set(state.foreign) | set(foreign_all)
        state.repairs = list(dict.fromkeys((state.repairs or []) + repairs_all))
        state.repairs_sample = sample_preview(
            [f"{b} → {g}" for (b, g) in state.repairs], 6
        )
        total_footnote = state.footnote_dupes + footnote_dupes

        existing = list(state.dropped or [])
        combined_map: dict[str, str] = {}
        for addr, reason in existing + dropped_current:
            if addr not in combined_map:
                combined_map[addr] = reason
        dropped_total = [(addr, combined_map[addr]) for addr in combined_map]

        report = await _compose_report_and_save(
            context,
            state.all_emails,
            state.to_send,
            dropped_total,
            sorted(foreign_total),
            total_footnote,
        )
        if state.repairs_sample:
            report += "\n\n🧩 Возможные исправления (проверьте вручную):"
            for s in state.repairs_sample:
                report += f"\n{s}"
        preview = context.chat_data.get("send_preview", {})
        dropped_preview = preview.get("dropped", [])
        fix_buttons: List[InlineKeyboardButton] = []
        for idx in range(min(len(dropped_preview), 5)):
            fix_buttons.append(
                InlineKeyboardButton(
                    f"✏️ Исправить №{idx + 1}", callback_data=f"fix:{idx}"
                )
            )

        extra_buttons: List[List[InlineKeyboardButton]] = []
        if fix_buttons:
            extra_buttons.append(fix_buttons)
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "🔁 Показать ещё примеры", callback_data="refresh_preview"
                )
            ]
        )
        if state.repairs:
            extra_buttons.append(
                [
                    InlineKeyboardButton(
                        f"🧩 Применить исправления ({len(state.repairs)})",
                        callback_data="apply_repairs",
                    )
                ]
            )
            extra_buttons.append(
                [
                    InlineKeyboardButton(
                        "🧩 Показать все исправления", callback_data="show_repairs"
                    )
                ]
            )
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    "▶️ Перейти к выбору направления", callback_data="proceed_group"
                )
            ]
        )
        report += "\n\nДополнительные действия:"
        await update.message.reply_text(
            report,
            reply_markup=InlineKeyboardMarkup(extra_buttons),
        )
        return


async def show_foreign_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show e-mail addresses with foreign domains."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    foreign = state.foreign if state else []
    if not foreign:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(foreign, 60):
        await query.message.reply_text("🌍 Иностранные домены:\n" + "\n".join(chunk))


async def apply_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Apply suggested address repairs to the send list."""

    query = update.callback_query
    state = get_state(context)
    repairs: List[tuple[str, str]] = state.repairs
    if not repairs:
        await query.answer("Нет кандидатов на исправление", show_alert=True)
        return
    await query.answer()
    current = set(state.to_send)
    applied = 0
    changed = []
    for bad, good in repairs:
        if bad in current:
            current.discard(bad)
            if is_allowed_tld(good):
                current.add(good)
                applied += 1
                if applied <= 12:
                    changed.append(f"{bad} → {good}")
    state.to_send = sorted(current)
    txt = f"🧩 Применено исправлений: {applied}."
    if changed:
        txt += "\n" + "\n".join(changed)
        if applied > len(changed):
            txt += f"\n… и ещё {applied - len(changed)}."
    await query.message.reply_text(txt)


async def show_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display all potential e-mail address repairs."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    repairs: List[tuple[str, str]] = state.repairs if state else []
    if not repairs:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    pairs = [f"{b} → {g}" for (b, g) in repairs]
    for chunk in _chunk_list(pairs, 60):
        await query.message.reply_text("🧩 Возможные исправления:\n" + "\n".join(chunk))


async def manual_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сброс состояния ручной рассылки."""

    query = update.callback_query
    await query.answer()
    clear_all_awaiting(context)
    init_state(context)
    context.chat_data.pop("manual_selected_template_code", None)
    context.chat_data.pop("manual_selected_template_label", None)
    await query.message.reply_text(
        "Сброшено. Нажмите /manual для новой ручной рассылки."
    )


async def send_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send e-mails entered manually by the user."""

    query = update.callback_query
    await query.answer()
    emails = context.chat_data.get("manual_all_emails") or []
    mode = context.chat_data.get("manual_send_mode", "allowed")
    data = query.data or ""
    _, _, group_raw = data.partition(":")
    group_code = _normalize_template_code(group_raw)
    template_info = get_template(group_code)
    template_path_obj = _template_path(template_info)
    if not template_info or not template_path_obj or not template_path_obj.exists():
        await query.message.reply_text(
            "⚠️ Шаблон не найден или файл отсутствует. Обновите список и попробуйте снова."
        )
        return
    template_path = str(template_path_obj)
    label = _template_label(template_info) or group_code

    enforce, days, allow_override = _manual_cfg()
    if enforce and mode == "allowed":
        allowed, rejected = _filter_by_180(list(emails), group_code, days)
        to_send = allowed
    else:
        to_send = list(emails)
        rejected = []

    # Если вообще нет исходных адресов — подскажем и выйдем
    if not emails:
        await query.message.reply_text(
            "Список адресов пуст. Нажмите /manual и введите адреса."
        )
        return

    # Сообщение без раскрытия адресов — только счётчики
    display_label = label
    if label.lower() != group_code:
        display_label = f"{label} ({group_code})"
    await query.message.reply_text(
        f"Шаблон: {display_label}\nК отправке: {len(to_send)}"
        + (f"\nОтфильтровано по правилу 180 дней: {len(rejected)}" if rejected else "")
    )

    # Если отправлять нечего (всё отфильтровано) — не запускаем рассылку
    if len(to_send) == 0:
        if allow_override and len(rejected) > 0:
            await query.message.reply_text(
                "Все адреса были отфильтрованы правилом 180 дней.\n"
                "Вы можете нажать «Отправить всем» для игнорирования правила."
            )
        else:
            await query.message.reply_text(
                "Все адреса были отфильтрованы правилом 180 дней. Отправка не запущена."
            )
        return

    # Сохраняем выбранный набор; дальнейшая логика подхватит эти значения
    context.chat_data["manual_selected_template_code"] = group_code
    context.chat_data["manual_selected_template_label"] = label
    context.chat_data["manual_selected_emails"] = to_send

    await query.message.reply_text("Запущено — выполняю в фоне...")

    async def long_job() -> None:
        chat_id = query.message.chat.id

        # manual отправка не учитывает супресс-лист
        get_blocked_emails()
        sent_today = get_sent_today()
        preview = context.chat_data.get("send_preview", {}) or {}
        fixed_map: Dict[str, str] = {}
        for item in preview.get("fixed", []):
            if isinstance(item, dict):
                new_addr = item.get("to")
                original_addr = item.get("from")
                if new_addr and original_addr:
                    fixed_map[str(new_addr)] = str(original_addr)

        try:
            imap = imaplib.IMAP4_SSL("imap.mail.ru")
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await query.message.reply_text(f"❌ IMAP ошибка: {e}")
            return

        to_send_local = list(to_send)

        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not is_force_send(chat_id):
            logger.info(
                "Daily limit reached: %s emails sent today (source=sent_log)",
                len(sent_today),
            )
            await update.callback_query.message.reply_text(
                (
                    f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                    "Если вы исправили ошибки — нажмите "
                    "«🚀 Игнорировать лимит» и запустите ещё раз."
                )
            )
            return
        if not is_force_send(chat_id) and len(to_send_local) > available:
            to_send_local = to_send_local[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )

        await query.message.reply_text(
            f"✉️ Рассылка начата. Отправляем {len(to_send_local)} писем..."
        )

        sent_count = 0
        errors: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        smtp = RobustSMTP()
        try:
            for email_addr in to_send_local:
                if cancel_event and cancel_event.is_set():
                    break
                try:
                    token = send_email_with_sessions(
                        smtp,
                        imap,
                        sent_folder,
                        email_addr,
                        template_path,
                        fixed_from=fixed_map.get(email_addr),
                    )
                    log_sent_email(
                        email_addr,
                        group_code,
                        "ok",
                        chat_id,
                        template_path,
                        unsubscribe_token=token,
                    )
                    sent_count += 1
                    await asyncio.sleep(1.5)
                except Exception as e:
                    errors.append(f"{email_addr} — {e}")
                    code, msg = None, None
                    if (
                        hasattr(e, "recipients")
                        and isinstance(e.recipients, dict)
                        and email_addr in e.recipients
                    ):
                        code, msg = (
                            e.recipients[email_addr][0],
                            e.recipients[email_addr][1],
                        )
                    elif hasattr(e, "smtp_code"):
                        code = getattr(e, "smtp_code", None)
                        msg = getattr(e, "smtp_error", None)
                    add_bounce(email_addr, code, str(msg or e), phase="send")
                    log_sent_email(
                        email_addr, group_code, "error", chat_id, template_path, str(e)
                    )
        finally:
            smtp.close()
        imap.logout()
        if cancel_event and cancel_event.is_set():
            await query.message.reply_text(
                f"Остановлено. Отправлено писем: {sent_count}"
            )
        else:
            await query.message.reply_text(f"✅ Отправлено писем: {sent_count}")
        if errors:
            await query.message.reply_text("Ошибки:\n" + "\n".join(errors))

        context.chat_data["manual_all_emails"] = []
        clear_recent_sent_cache()
        disable_force_send(chat_id)

    messaging.create_task_with_logging(long_job(), query.message.reply_text)


async def autosync_imap_with_message(query: CallbackQuery) -> None:
    """Synchronize IMAP logs and notify the user via message."""
    await query.answer()
    await query.message.reply_text("🔄 Синхронизация истории отправки с сервером...")
    loop = asyncio.get_running_loop()
    stats = await loop.run_in_executor(None, sync_log_with_imap)
    clear_recent_sent_cache()
    await query.message.reply_text(
        "✅ Синхронизация завершена. "
        f"новых: {stats['new_contacts']}, обновлено: {stats['updated_contacts']}, "
        f"пропущено: {stats['skipped_events']}, всего: {stats['total_rows_after']}.\n"
        f"История отправки обновлена на последние 6 месяцев."
    )


def _chunk_list(items: List[str], size: int = 60) -> List[List[str]]:
    """Split ``items`` into chunks of ``size`` elements."""

    return [items[i : i + size] for i in range(0, len(items), size)]


__all__ = [
    "start",
    "prompt_upload",
    "about_bot",
    "add_block_prompt",
    "show_blocked_list",
    "prompt_change_group",
    "force_send_command",
    "report_command",
    "report_callback",
    "sync_imap_command",
    "sync_bounces_command",
    "reset_email_list",
    "diag",
    "dedupe_log_command",
    "handle_document",
    "refresh_preview",
    "proceed_to_group",
    "select_group",
    "prompt_manual_email",
    "handle_text",
    "request_fix",
    "show_foreign_list",
    "apply_repairs",
    "show_repairs",
    "manual_mode",
    "manual_reset",
    "send_manual_email",
    "send_all",
    "autosync_imap_with_message",
]
