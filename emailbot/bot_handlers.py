"""Telegram bot handlers."""

from __future__ import annotations

import asyncio
import csv
import imaplib
import logging
import os
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Set

import aiohttp
from telegram import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import ContextTypes

from . import messaging
from . import extraction as _extraction
from .extraction import normalize_email, smart_extract_emails, extract_emails_manual
from .reporting import build_mass_report_text
from . import settings


def _preclean_text_for_emails(text: str) -> str:
    return text


def apply_numeric_truncation_removal(allowed):
    return allowed, []


async def async_extract_emails_from_url(url: str, session, chat_id=None):
    hits, stats = await asyncio.to_thread(_extraction.extract_from_url, url)
    emails = set(h.email.lower().strip() for h in hits)
    foreign = {e for e in emails if not is_allowed_tld(e)}
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
    tld = email_addr.rsplit(".", 1)[-1].lower()
    return tld in {"ru", "com"}


def is_numeric_localpart(email_addr: str) -> bool:
    local = email_addr.split("@", 1)[0]
    return local.isdigit()


def sample_preview(items, k: int):
    lst = list(dict.fromkeys(items))
    if len(lst) <= k:
        return lst
    return lst[:k]
from .messaging import (
    DOWNLOAD_DIR,
    LOG_FILE,
    MAX_EMAILS_PER_DAY,
    TEMPLATE_MAP,
    add_blocked_email,
    clear_recent_sent_cache,
    dedupe_blocked_file,
    get_blocked_emails,
    get_preferred_sent_folder,
    get_sent_today,
    log_sent_email,
    send_email_with_sessions,
    sync_log_with_imap,
    was_emailed_recently,
    count_sent_today,
)
from .smtp_client import SmtpClient
from .utils import log_error
from .messaging_utils import (
    add_bounce,
    is_foreign,
    is_hard_bounce,
    is_soft_bounce,
    is_suppressed,
    suppress_add,
    was_sent_within,
    BOUNCE_LOG_PATH,
)

logger = logging.getLogger(__name__)

ADMIN_IDS = {
    int(x)
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

PREVIEW_ALLOWED = 10
PREVIEW_NUMERIC = 6
PREVIEW_FOREIGN = 6

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
    suspect_numeric: List[str] = field(default_factory=list)
    foreign: List[str] = field(default_factory=list)
    preview_allowed_all: List[str] = field(default_factory=list)
    repairs: List[tuple[str, str]] = field(default_factory=list)
    repairs_sample: List[str] = field(default_factory=list)
    group: Optional[str] = None
    template: Optional[str] = None
    footnote_dupes: int = 0


FORCE_SEND_CHAT_IDS: set[int] = set()
SESSION_KEY = "state"


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
        return

    settings.load()

    def _status() -> str:
        return (
            f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}\n"
            f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}\n"
            f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}\n"
            f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        )

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'строгий' if settings.STRICT_OBFUSCATION else 'обычный'} ⏼",
                        callback_data="feature_strict",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"Сноски: радиус {settings.FOOTNOTE_RADIUS_PAGES}",
                        callback_data="feature_footnote",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feature_pdf",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feature_ocr",
                    )
                ],
            ]
        )

    await update.message.reply_text(_status(), reply_markup=_keyboard())


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
    if data == "feature_strict":
        settings.STRICT_OBFUSCATION = not settings.STRICT_OBFUSCATION
    elif data == "feature_footnote":
        settings.FOOTNOTE_RADIUS_PAGES = (settings.FOOTNOTE_RADIUS_PAGES + 1) % 3
    elif data == "feature_pdf":
        settings.PDF_LAYOUT_AWARE = not settings.PDF_LAYOUT_AWARE
    elif data == "feature_ocr":
        settings.ENABLE_OCR = not settings.ENABLE_OCR
    settings.save()

    def _status() -> str:
        return (
            f"STRICT_OBFUSCATION={'on' if settings.STRICT_OBFUSCATION else 'off'}\n"
            f"FOOTNOTE_RADIUS_PAGES={settings.FOOTNOTE_RADIUS_PAGES}\n"
            f"PDF_LAYOUT_AWARE={'on' if settings.PDF_LAYOUT_AWARE else 'off'}\n"
            f"ENABLE_OCR={'on' if settings.ENABLE_OCR else 'off'}"
        )

    def _keyboard() -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"Обфускации: {'строгий' if settings.STRICT_OBFUSCATION else 'обычный'} ⏼",
                        callback_data="feature_strict",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"Сноски: радиус {settings.FOOTNOTE_RADIUS_PAGES}",
                        callback_data="feature_footnote",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"PDF-layout {'on' if settings.PDF_LAYOUT_AWARE else 'off'} ⏼",
                        callback_data="feature_pdf",
                    )
                ],
                [
                    InlineKeyboardButton(
                        f"OCR {'on' if settings.ENABLE_OCR else 'off'} ⏼",
                        callback_data="feature_ocr",
                    )
                ],
            ]
        )

    await query.answer()
    await query.edit_message_text(_status(), reply_markup=_keyboard())


async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin diagnostic command with runtime information."""

    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return

    import sys
    import csv
    import telegram
    import aiohttp
    from datetime import datetime

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu and initialize state."""

    init_state(context)
    keyboard = [
        ["📤 Массовая", "🛑 Стоп", "✉️ Ручная"],
        ["🧹 Очистить список", "📄 Показать исключения"],
        ["🚫 Добавить в исключения", "🧾 О боте"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🔄 Синхронизировать с сервером", "🚀 Игнорировать лимит"],
    ]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Можно загрузить данные", reply_markup=markup)


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

    keyboard = [
        [InlineKeyboardButton("⚽ Спорт", callback_data="group_спорт")],
        [InlineKeyboardButton("🏕 Туризм", callback_data="group_туризм")],
        [InlineKeyboardButton("🩺 Медицина", callback_data="group_медицина")],
    ]
    await update.message.reply_text(
        "⬇️ Выберите направление рассылки:",
        reply_markup=InlineKeyboardMarkup(keyboard),
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

    if not os.path.exists(LOG_FILE):
        return "Нет данных о рассылках."
    now = datetime.now()
    if period == "day":
        start_at = now - timedelta(days=1)
    elif period == "week":
        start_at = now - timedelta(weeks=1)
    elif period == "month":
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
    await query.edit_message_text(f"📊 {mapping.get(period, period)}:\n{text}")


async def sync_imap_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Synchronize local log with the IMAP "Sent" folder."""

    await update.message.reply_text(
        "⏳ Сканируем папку «Отправленные» (последние 180 дней)..."
    )
    try:
        added = sync_log_with_imap()
        clear_recent_sent_cache()
        await update.message.reply_text(f"🔄 Добавлено в лог {added} новых адресов.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка синхронизации: {e}")


async def retry_last_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    init_state(context)
    context.user_data.pop("manual_emails", None)
    await update.message.reply_text(
        "Список email-адресов и файлов очищен. Можно загружать новые файлы!"
    )


async def _compose_report_and_save(
    context: ContextTypes.DEFAULT_TYPE,
    allowed_all: Set[str],
    filtered: List[str],
    suspicious_numeric: List[str],
    foreign: List[str],
    footnote_dupes: int = 0,
) -> str:
    """Compose a summary report and store samples in session state."""

    state = get_state(context)
    state.preview_allowed_all = sorted(allowed_all)
    state.suspect_numeric = suspicious_numeric
    state.foreign = sorted(foreign)
    state.footnote_dupes = footnote_dupes

    sample_allowed = sample_preview(state.preview_allowed_all, PREVIEW_ALLOWED)
    sample_numeric = (
        sample_preview(suspicious_numeric, PREVIEW_NUMERIC)
        if suspicious_numeric
        else []
    )
    sample_foreign = sample_preview(state.foreign, PREVIEW_FOREIGN)

    report = (
        "✅ Анализ завершён.\n"
        f"Найдено адресов: {len(allowed_all)}\n"
        f"Уникальных (после очистки): {len(filtered)}\n"
        f"Подозрительные (логин только из цифр): {len(suspicious_numeric)}\n"
        f"Иностранные домены: {len(foreign)}"
    )
    if footnote_dupes:
        report += f"\nВозможные сносочные дубликаты удалены: {footnote_dupes}"
    if sample_allowed:
        report += "\n\n🧪 Примеры:\n" + "\n".join(sample_allowed)
    if sample_numeric:
        report += "\n\n🔢 Примеры цифровых:\n" + "\n".join(sample_numeric)
    if sample_foreign:
        report += "\n\n🌍 Примеры иностранных:\n" + "\n".join(
            sample_foreign
        )
    return report


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
    progress_msg = await update.message.reply_text("🔎 Анализируем...")

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
            footnote_dupes += stats.get("footnote_trimmed_merged", 0)
        else:
            allowed, loose, stats = extract_from_uploaded_file(file_path)
            allowed_all.update(allowed)
            loose_all.update(loose)
            extracted_files.append(file_path)
            repairs = collect_repairs_from_files([file_path])
            footnote_dupes += stats.get("footnote_trimmed_merged", 0)
    except Exception as e:
        log_error(f"handle_document: {file_path}: {e}")

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(repairs + trunc_pairs))

    technical_emails = [e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)]
    filtered = [
        e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)
    ]

    suspicious_numeric = sorted({e for e in filtered if is_numeric_localpart(e)})

    foreign_raw = {e for e in loose_all if not is_allowed_tld(e)}
    foreign = sorted(collapse_footnote_variants(foreign_raw))

    state = get_state(context)
    state.all_emails = set(filtered)
    state.all_files = extracted_files
    state.to_send = sorted(set(filtered))
    state.repairs = repairs
    state.repairs_sample = sample_preview([f"{b} → {g}" for (b, g) in state.repairs], 6)

    report = await _compose_report_and_save(
        context, allowed_all, filtered, suspicious_numeric, foreign, footnote_dupes
    )
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for s in state.repairs_sample:
            report += f"\n{s}"
    extra_buttons = [
        [
            InlineKeyboardButton(
                "🔁 Показать ещё примеры", callback_data="refresh_preview"
            )
        ]
    ]
    # No extra buttons for numeric or foreign preview
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
    numeric = state.suspect_numeric if state else []
    foreign = state.foreign if state else []
    if not (allowed_all or numeric or foreign):
        await query.answer(
            "Нет данных для примеров. Загрузите файл/ссылки.", show_alert=True
        )
        return
    await query.answer()
    sample_allowed = sample_preview(allowed_all, PREVIEW_ALLOWED)
    sample_numeric = sample_preview(numeric, PREVIEW_NUMERIC)
    sample_foreign = sample_preview(foreign, PREVIEW_FOREIGN)
    report = []
    if sample_allowed:
        report.append("🧪 Примеры:\n" + "\n".join(sample_allowed))
    if sample_numeric:
        report.append("🔢 Примеры цифровых:\n" + "\n".join(sample_numeric))
    if sample_foreign:
        report.append("🌍 Примеры иностранных:\n" + "\n".join(sample_foreign))
    await query.message.reply_text(
        "\n\n".join(report) if report else "Показать нечего."
    )


async def proceed_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch to the mailing group selection step."""

    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("⚽ Спорт", callback_data="group_спорт")],
        [InlineKeyboardButton("🏕 Туризм", callback_data="group_туризм")],
        [InlineKeyboardButton("🩺 Медицина", callback_data="group_медицина")],
    ]
    await query.message.reply_text(
        "⬇️ Выберите направление рассылки:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle group selection and prepare messages for sending."""

    query = update.callback_query
    await query.answer()
    group_code = query.data.split("_")[1]
    template_path = TEMPLATE_MAP[group_code]
    state = get_state(context)
    emails = state.to_send
    state.group = group_code
    state.template = template_path
    await query.message.reply_text(
        (
            f"✉️ Готово к отправке {len(emails)} писем.\n"
            "Для запуска рассылки нажмите кнопку ниже."
        ),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("✉️ Начать рассылку", callback_data="start_sending")]]
        ),
    )


async def prompt_manual_email(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask the user to enter e-mail addresses manually."""

    clear_all_awaiting(context)
    context.user_data.pop("manual_emails", None)
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
        found = extract_emails_manual(text)
        filtered = sorted(set(e.lower().strip() for e in found))
        logger.info(
            "Manual input parsing: raw=%r found=%r filtered=%r",
            text,
            found,
            filtered,
        )
        if filtered:
            context.user_data["manual_emails"] = sorted(filtered)
            context.user_data["awaiting_manual_email"] = False
            keyboard = [
                [InlineKeyboardButton("⚽ Спорт", callback_data="manual_group_спорт")],
                [InlineKeyboardButton("🏕 Туризм", callback_data="manual_group_туризм")],
                [
                    InlineKeyboardButton(
                        "🩺 Медицина", callback_data="manual_group_медицина"
                    )
                ],
            ]
            await update.message.reply_text(
                (
                    f"К отправке: {', '.join(context.user_data['manual_emails'])}\n\n"
                    "⬇️ Выберите направление письма:"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await update.message.reply_text("❌ Не найдено ни одного email.")
        return

    urls = re.findall(r"https?://\S+", text)
    if urls:
        await update.message.reply_text("🌐 Загружаем страницы...")
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = [
                async_extract_emails_from_url(url, session, chat_id) for url in urls
            ]
            results = await asyncio.gather(*tasks)
        allowed_all: Set[str] = set()
        foreign_all: Set[str] = set()
        repairs_all: List[tuple[str, str]] = []
        footnote_dupes = 0
        for _, allowed, foreign, repairs, stats in results:
            allowed_all.update(allowed)
            foreign_all.update(foreign)
            repairs_all.extend(repairs)
            footnote_dupes += stats.get("footnote_trimmed_merged", 0)

        technical_emails = [
            e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)
        ]
        filtered = sorted(e for e in allowed_all if e not in technical_emails)
        suspicious_numeric = sorted({e for e in filtered if is_numeric_localpart(e)})

        state = get_state(context)
        current = set(state.to_send)
        current.update(filtered)
        state.to_send = sorted(current)
        state.foreign = sorted(foreign_all)
        state.repairs = list(dict.fromkeys((state.repairs or []) + repairs_all))
        state.repairs_sample = sample_preview(
            [f"{b} → {g}" for (b, g) in state.repairs], 6
        )

        report = await _compose_report_and_save(
            context, allowed_all, filtered, suspicious_numeric, foreign_all, footnote_dupes
        )
        if state.repairs_sample:
            report += "\n\n🧩 Возможные исправления (проверьте вручную):"
            for s in state.repairs_sample:
                report += f"\n{s}"
        extra_buttons = [
            [
                InlineKeyboardButton(
                    "🔁 Показать ещё примеры", callback_data="refresh_preview"
                )
            ]
        ]
        # No extra buttons for numeric or foreign preview
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


async def ask_include_numeric(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Ask whether numeric-only addresses should be added."""

    query = update.callback_query
    state = get_state(context)
    numeric = state.suspect_numeric
    if not numeric:
        await query.answer("Цифровых адресов нет", show_alert=True)
        return
    await query.answer()
    preview_list = numeric[:60]
    txt = (
        f"Найдено цифровых логинов: {len(numeric)}.\nБудут добавлены все.\n\nПример:\n"
        + "\n".join(preview_list)
    )
    more = len(numeric) - len(preview_list)
    if more > 0:
        txt += f"\n… и ещё {more}."
    await query.message.reply_text(
        txt,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Включить все цифровые",
                        callback_data="confirm_include_numeric",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "↩️ Отмена", callback_data="cancel_include_numeric"
                    )
                ],
            ]
        ),
    )


async def include_numeric_emails(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Include numeric-only e-mail addresses in the send list."""

    query = update.callback_query
    state = get_state(context)
    numeric = state.suspect_numeric
    if not numeric:
        await query.answer("Цифровых адресов нет", show_alert=True)
        return
    await query.answer()
    current = set(state.to_send)
    added = [e for e in numeric if e not in current]
    current.update(numeric)
    state.to_send = sorted(current)
    await query.message.reply_text(
        (
            f"➕ Добавлено цифровых адресов: {len(added)}.\n"
            f"Итого к отправке: {len(state.to_send)}."
        )
    )


async def cancel_include_numeric(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Keep numeric addresses excluded from the send list."""

    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Ок, цифровые адреса оставлены выключенными.")


async def show_numeric_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display a list of numeric-only e-mail addresses."""

    query = update.callback_query
    state = context.chat_data.get(SESSION_KEY)
    numeric = state.suspect_numeric if state else []
    if not numeric:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(numeric, 60):
        await query.message.reply_text("🔢 Цифровые логины:\n" + "\n".join(chunk))


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
        await query.message.reply_text(
            "🌍 Иностранные домены:\n" + "\n".join(chunk)
        )


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


async def send_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send e-mails entered manually by the user."""

    query = update.callback_query
    await query.answer()
    emails = context.user_data.get("manual_emails", [])
    if not emails:
        await query.message.reply_text("❗ Список email пуст.")
        return

    await query.message.reply_text("Запущено — выполняю в фоне...")

    async def long_job() -> None:
        chat_id = query.message.chat.id
        group_code = query.data.split("_")[2]
        template_path = TEMPLATE_MAP[group_code]

        # manual отправка не учитывает супресс-лист
        get_blocked_emails()
        sent_today = get_sent_today()

        try:
            imap = imaplib.IMAP4_SSL("imap.mail.ru")
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await query.message.reply_text(f"❌ IMAP ошибка: {e}")
            return

        to_send = list(emails)

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
        if not is_force_send(chat_id) and len(to_send) > available:
            to_send = to_send[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )

        await query.message.reply_text(
            f"✉️ Рассылка начата. Отправляем {len(to_send)} писем..."
        )

        sent_count = 0
        errors: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        with SmtpClient(
            "smtp.mail.ru", 465, messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD
        ) as client:
            for email_addr in to_send:
                if cancel_event and cancel_event.is_set():
                    break
                try:
                    token = send_email_with_sessions(
                        client, imap, sent_folder, email_addr, template_path
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
                        code, msg = e.recipients[email_addr][0], e.recipients[email_addr][1]
                    elif hasattr(e, "smtp_code"):
                        code = getattr(e, "smtp_code", None)
                        msg = getattr(e, "smtp_error", None)
                    add_bounce(email_addr, code, str(msg or e), phase="send")
                    log_sent_email(
                        email_addr, group_code, "error", chat_id, template_path, str(e)
                    )
        imap.logout()
        if cancel_event and cancel_event.is_set():
            await query.message.reply_text(
                f"Остановлено. Отправлено писем: {sent_count}"
            )
        else:
            await query.message.reply_text(f"✅ Отправлено писем: {sent_count}")
        if errors:
            await query.message.reply_text("Ошибки:\n" + "\n".join(errors))

        context.user_data["manual_emails"] = []
        clear_recent_sent_cache()
        disable_force_send(chat_id)

    messaging.create_task_with_logging(long_job(), query.message.reply_text)


async def send_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send all prepared e-mails respecting limits."""

    query = update.callback_query
    state = get_state(context)
    emails = state.to_send
    group_code = state.group
    template_path = state.template
    if not emails or not group_code or not template_path:
        await query.answer("Нет данных для отправки", show_alert=True)
        return
    await query.answer()
    await query.message.reply_text("Запущено — выполняю в фоне...")

    async def long_job() -> None:
        chat_id = query.message.chat.id
        lookup_days = int(os.getenv("EMAIL_LOOKBACK_DAYS", "180"))
        blocked = get_blocked_emails()
        sent_today = get_sent_today()

        blocked_foreign: List[str] = []
        blocked_invalid: List[str] = []
        skipped_recent: List[str] = []
        to_send: List[str] = []
        sent_ok: List[str] = []

        initial = [e for e in emails if e not in blocked and e not in sent_today]
        for e in initial:
            if is_foreign(e):
                blocked_foreign.append(e)
            else:
                to_send.append(e)

        queue: List[str] = []
        for e in to_send:
            if is_suppressed(e):
                blocked_invalid.append(e)
            else:
                queue.append(e)

        to_send = []
        for e in queue:
            if was_sent_within(e, days=lookup_days):
                skipped_recent.append(e)
            else:
                to_send.append(e)

        if not to_send:
            await query.message.reply_text(
                "❗ Все адреса уже есть в истории отправок или в блок-листах."
            )
            return

        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not is_force_send(chat_id):
            logger.info(
                "Daily limit reached: %s emails sent today (source=sent_log)",
                len(sent_today),
            )
            await query.message.reply_text(
                (
                    f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                    "Если вы исправили ошибки — нажмите "
                    "«🚀 Игнорировать лимит» и запустите ещё раз."
                )
            )
            return
        if not is_force_send(chat_id) and len(to_send) > available:
            to_send = to_send[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )

        await query.message.reply_text(
            f"✉️ Рассылка начата. Отправляем {len(to_send)} писем..."
        )

        try:
            imap = imaplib.IMAP4_SSL("imap.mail.ru")
            imap.login(messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD)
            sent_folder = get_preferred_sent_folder(imap)
            imap.select(f'"{sent_folder}"')
        except Exception as e:
            log_error(f"imap connect: {e}")
            await query.message.reply_text(f"❌ IMAP ошибка: {e}")
            return

        errors: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        with SmtpClient(
            "smtp.mail.ru", 465, messaging.EMAIL_ADDRESS, messaging.EMAIL_PASSWORD
        ) as client:
            for email_addr in to_send:
                if cancel_event and cancel_event.is_set():
                    break
                try:
                    token = send_email_with_sessions(
                        client, imap, sent_folder, email_addr, template_path
                    )
                    log_sent_email(
                        email_addr,
                        group_code,
                        "ok",
                        chat_id,
                        template_path,
                        unsubscribe_token=token,
                    )
                    sent_ok.append(email_addr)
                    await asyncio.sleep(1.5)
                except Exception as e:
                    errors.append(f"{email_addr} — {e}")
                    code, msg = None, None
                    if (
                        hasattr(e, "recipients")
                        and isinstance(e.recipients, dict)
                        and email_addr in e.recipients
                    ):
                        code, msg = e.recipients[email_addr][0], e.recipients[email_addr][1]
                    elif hasattr(e, "smtp_code"):
                        code = getattr(e, "smtp_code", None)
                        msg = getattr(e, "smtp_error", None)
                    add_bounce(email_addr, code, str(msg or e), phase="send")
                    if is_hard_bounce(code, msg):
                        suppress_add(email_addr, code, "hard bounce on send")
                    log_sent_email(
                        email_addr, group_code, "error", chat_id, template_path, str(e)
                    )
        imap.logout()

        report_text = build_mass_report_text(
            sent_ok,
            skipped_recent,
            blocked_foreign,
            blocked_invalid,
        )

        await query.message.reply_text(report_text)
        if errors:
            await query.message.reply_text("Ошибки:\n" + "\n".join(errors))

        clear_recent_sent_cache()
        disable_force_send(chat_id)

    messaging.create_task_with_logging(long_job(), query.message.reply_text)


async def autosync_imap_with_message(query: CallbackQuery) -> None:
    """Synchronize IMAP logs and notify the user via message."""
    await query.answer()
    await query.message.reply_text("🔄 Синхронизация истории отправки с сервером...")
    loop = asyncio.get_running_loop()
    added = await loop.run_in_executor(None, sync_log_with_imap)
    clear_recent_sent_cache()
    await query.message.reply_text(
        f"✅ Синхронизация завершена. В лог добавлено новых адресов: {added}.\n"
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
    "reset_email_list",
    "diag",
    "handle_document",
    "refresh_preview",
    "proceed_to_group",
    "select_group",
    "prompt_manual_email",
    "handle_text",
    "ask_include_numeric",
    "include_numeric_emails",
    "cancel_include_numeric",
    "show_numeric_list",
    "show_foreign_list",
    "apply_repairs",
    "show_repairs",
    "send_manual_email",
    "send_all",
    "autosync_imap_with_message",
]
