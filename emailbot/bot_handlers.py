"""Telegram bot handlers."""

from __future__ import annotations

import os
import time
import re
import csv
import asyncio
from datetime import datetime, timedelta
from typing import Set, List, Optional

from dataclasses import dataclass, field

import aiohttp
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import ContextTypes, filters

from .extraction import (
    normalize_email,
    extract_clean_emails_from_text,
    extract_emails_loose,
    extract_from_uploaded_file,
    extract_emails_from_zip,
    apply_numeric_truncation_removal,
    is_allowed_tld,
    is_numeric_localpart,
    collapse_footnote_variants,
    find_prefix_repairs,
    collect_repairs_from_files,
    sample_preview,
    async_extract_emails_from_url,
)
from .messaging import (
    DOWNLOAD_DIR,
    LOG_FILE,
    MAX_EMAILS_PER_DAY,
    TEMPLATE_MAP,
    async_send_email,
    add_blocked_email,
    dedupe_blocked_file,
    get_blocked_emails,
    log_sent_email,
    get_sent_today,
    get_recent_6m_union,
    clear_recent_sent_cache,
    sync_log_with_imap,
)
from .utils import log_error


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
    manual_emails: List[str] = field(default_factory=list)


FORCE_SEND_CHAT_IDS: set[int] = set()
session_data: dict[int, SessionState] = {}


def enable_force_send(chat_id: int) -> None:
    FORCE_SEND_CHAT_IDS.add(chat_id)


def disable_force_send(chat_id: int) -> None:
    FORCE_SEND_CHAT_IDS.discard(chat_id)


def is_force_send(chat_id: int) -> bool:
    return chat_id in FORCE_SEND_CHAT_IDS


def clear_all_awaiting(context: ContextTypes.DEFAULT_TYPE):
    for key in ["awaiting_block_email", "awaiting_manual_email"]:
        context.user_data[key] = False


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["📤 Загрузить данные для поиска контактов", "🧹 Очистить список"],
        ["📄 Показать исключения", "🚫 Добавить в исключения"],
        ["✉️ Ручная рассылка", "🧾 О боте"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🔄 Синхронизировать с сервером", "🚀 Игнорировать лимит"],
    ]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Можно загрузить данные", reply_markup=markup)


async def prompt_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📥 Загрузите данные с e-mail-адресами для рассылки.\n\n"
        "Поддерживаемые форматы: PDF, Excel (.xlsx), Word (.docx), CSV, ZIP (с этими файлами внутри), а также ссылки на сайты."
    )


async def about_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Бот делает рассылку HTML-писем с учётом истории отправки (IMAP 180 дней) и блок-листа. "
        "Один адрес — не чаще 1 раза в 6 месяцев. Домены: только .ru и .com."
    )


async def add_block_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_all_awaiting(context)
    await update.message.reply_text(
        "Введите email или список email-адресов (через запятую/пробел/с новой строки), которые нужно добавить в исключения:"
    )
    context.user_data["awaiting_block_email"] = True


async def show_blocked_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dedupe_blocked_file()
    blocked = get_blocked_emails()
    if not blocked:
        await update.message.reply_text("📄 Список исключений пуст.")
    else:
        await update.message.reply_text(
            "📄 В исключениях:\n" + "\n".join(sorted(blocked))
        )


async def prompt_change_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("⚽ Спорт", callback_data="group_спорт")],
        [InlineKeyboardButton("🏕 Туризм", callback_data="group_туризм")],
        [InlineKeyboardButton("🩺 Медицина", callback_data="group_медицина")],
    ]
    await update.message.reply_text(
        "⬇️ Выберите направление рассылки:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def force_send_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    enable_force_send(chat_id)
    await update.message.reply_text(
        "Режим игнорирования дневного лимита включён для этого чата.\n"
        "Запустите рассылку ещё раз — ограничение на сегодня будет проигнорировано."
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📆 День", callback_data="report_day")],
        [InlineKeyboardButton("🗓 Неделя", callback_data="report_week")],
        [InlineKeyboardButton("🗓 Месяц", callback_data="report_month")],
        [InlineKeyboardButton("📅 Год", callback_data="report_year")],
    ]
    await update.message.reply_text(
        "Выберите период отчёта:", reply_markup=InlineKeyboardMarkup(keyboard)
    )


def get_report(period="day"):
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


async def report_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    period = query.data.replace("report_", "")
    mapping = {
        "day": "Отчёт за день",
        "week": "Отчёт за неделю",
        "month": "Отчёт за месяц",
        "year": "Отчёт за год",
    }
    text = get_report(period)
    await query.edit_message_text(f"📊 {mapping.get(period, period)}:\n{text}")


async def sync_imap_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⏳ Сканируем папку «Отправленные» (последние 180 дней)..."
    )
    try:
        added = sync_log_with_imap()
        clear_recent_sent_cache()
        await update.message.reply_text(f"🔄 Добавлено в лог {added} новых адресов.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка синхронизации: {e}")


async def reset_email_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    state = session_data.setdefault(chat_id, SessionState())
    state.all_emails.clear()
    state.all_files.clear()
    state.to_send.clear()
    state.suspect_numeric.clear()
    state.foreign.clear()
    state.preview_allowed_all.clear()
    state.repairs.clear()
    state.repairs_sample.clear()
    await update.message.reply_text(
        "Список email-адресов и файлов очищен. Можно загружать новые файлы!"
    )


async def _compose_report_and_save(
    chat_id: int,
    allowed_all: Set[str],
    filtered: List[str],
    suspicious_numeric: List[str],
    foreign: List[str],
) -> str:
    state = session_data.setdefault(chat_id, SessionState())
    state.preview_allowed_all = sorted(allowed_all)
    state.suspect_numeric = suspicious_numeric
    state.foreign = sorted(foreign)

    sample_allowed = sample_preview(state.preview_allowed_all, PREVIEW_ALLOWED)
    sample_numeric = sample_preview(suspicious_numeric, PREVIEW_NUMERIC)
    sample_foreign = sample_preview(state.foreign, PREVIEW_FOREIGN)

    report = (
        "✅ Анализ завершён.\n"
        f"Найдено адресов (.ru/.com): {len(allowed_all)}\n"
        f"Уникальных (после базовой очистки): {len(filtered)}\n"
        f"Подозрительные (логин только из цифр, исключены): {len(suspicious_numeric)}\n"
        f"Иностранные домены (исключены): {len(foreign)}"
    )
    if sample_allowed:
        report += "\n\n🧪 Примеры (.ru/.com):\n" + "\n".join(sample_allowed)
    if sample_numeric:
        report += "\n\n🔢 Примеры цифровых (исключены):\n" + "\n".join(sample_numeric)
    if sample_foreign:
        report += "\n\n🌍 Примеры иностранных (исключены):\n" + "\n".join(sample_foreign)
    return report


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    try:
        if file_path.lower().endswith(".zip"):
            allowed_all, extracted_files, loose_all = await extract_emails_from_zip(
                file_path, progress_msg, DOWNLOAD_DIR
            )
            repairs = collect_repairs_from_files(extracted_files)
        else:
            allowed, loose = extract_from_uploaded_file(file_path)
            allowed_all.update(allowed)
            loose_all.update(loose)
            extracted_files.append(file_path)
            repairs = collect_repairs_from_files([file_path])
    except Exception as e:
        log_error(f"handle_document: {file_path}: {e}")

    allowed_all, trunc_pairs = apply_numeric_truncation_removal(allowed_all)
    repairs = list(dict.fromkeys(repairs + trunc_pairs))

    technical_emails = [e for e in allowed_all if any(tp in e for tp in TECH_PATTERNS)]
    filtered = [e for e in allowed_all if e not in technical_emails and is_allowed_tld(e)]

    suspicious_numeric = sorted({e for e in filtered if is_numeric_localpart(e)})
    filtered = [e for e in filtered if not is_numeric_localpart(e)]

    foreign_raw = {e for e in loose_all if not is_allowed_tld(e)}
    foreign = sorted(collapse_footnote_variants(foreign_raw))

    state = session_data.setdefault(chat_id, SessionState())
    state.all_emails = set(filtered)
    state.all_files = extracted_files
    state.to_send = sorted(set(filtered))
    state.repairs = repairs
    state.repairs_sample = sample_preview(
        [f"{b} → {g}" for (b, g) in repairs], 6
    )

    report = await _compose_report_and_save(
        chat_id, allowed_all, filtered, suspicious_numeric, foreign
    )
    if state.repairs_sample:
        report += "\n\n🧩 Возможные исправления (проверьте вручную):"
        for s in state.repairs_sample:
            report += f"\n{s}"
    await update.message.reply_text(report)

    extra_buttons = [
        [InlineKeyboardButton("🔁 Показать ещё примеры", callback_data="refresh_preview")]
    ]
    if suspicious_numeric:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    f"➕ Включить цифровые ({len(suspicious_numeric)})",
                    callback_data="ask_include_numeric",
                )
            ]
        )
        extra_buttons.append(
            [InlineKeyboardButton("🔢 Показать цифровые", callback_data="show_numeric")]
        )
    if state.foreign:
        extra_buttons.append(
            [
                InlineKeyboardButton(
                    f"🌍 Показать иностранные ({len(state.foreign)})",
                    callback_data="show_foreign",
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
            [InlineKeyboardButton("🧩 Показать все исправления", callback_data="show_repairs")]
        )
    extra_buttons.append(
        [InlineKeyboardButton("▶️ Перейти к выбору направления", callback_data="proceed_group")]
    )

    await update.message.reply_text(
        "Дополнительные действия:",
        reply_markup=InlineKeyboardMarkup(extra_buttons),
    )


async def refresh_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.get(chat_id)
    allowed_all = state.preview_allowed_all if state else []
    numeric = state.suspect_numeric if state else []
    foreign = state.foreign if state else []
    if not (allowed_all or numeric or foreign):
        await query.answer("Нет данных для примеров. Загрузите файл/ссылки.", show_alert=True)
        return
    await query.answer()
    sample_allowed = sample_preview(allowed_all, PREVIEW_ALLOWED)
    sample_numeric = sample_preview(numeric, PREVIEW_NUMERIC)
    sample_foreign = sample_preview(foreign, PREVIEW_FOREIGN)
    report = []
    if sample_allowed:
        report.append("🧪 Примеры (.ru/.com):\n" + "\n".join(sample_allowed))
    if sample_numeric:
        report.append("🔢 Примеры цифровых (исключены):\n" + "\n".join(sample_numeric))
    if sample_foreign:
        report.append("🌍 Примеры иностранных (исключены):\n" + "\n".join(sample_foreign))
    await query.message.reply_text("\n\n".join(report) if report else "Показать нечего.")


async def proceed_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
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


async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    group_code = query.data.split("_")[1]
    template_path = TEMPLATE_MAP[group_code]
    chat_id = query.message.chat.id
    state = session_data.setdefault(chat_id, SessionState())
    emails = state.to_send
    state.group = group_code
    state.template = template_path
    await query.message.reply_text(
        f"✉️ Готово к отправке {len(emails)} писем.\n"
        f"Для запуска рассылки нажмите кнопку ниже.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("✉️ Начать рассылку", callback_data="start_sending")]]
        ),
    )


async def prompt_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_all_awaiting(context)
    await update.message.reply_text(
        "Введите email или список email-адресов (через запятую/пробел/с новой строки):"
    )
    context.user_data["awaiting_manual_email"] = True


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text or ""
    if context.user_data.get("awaiting_block_email"):
        emails = {
            normalize_email(x)
            for x in extract_emails_loose(text)
            if "@" in x
        }
        added = [e for e in emails if add_blocked_email(e)]
        await update.message.reply_text(
            f"Добавлено в исключения: {len(added)}" if added else "Ничего не добавлено."
        )
        context.user_data["awaiting_block_email"] = False
        return
    if context.user_data.get("awaiting_manual_email"):
        state = session_data.setdefault(chat_id, SessionState())
        state.manual_emails.append(text)
        await update.message.reply_text(
            "Добавлено. Введите ещё или нажмите кнопку «✉️ Ручная рассылка»."
        )
        return

    urls = re.findall(r"https?://\S+", text)
    if urls:
        await update.message.reply_text("🌐 Загружаем страницы...")
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = [async_extract_emails_from_url(url, session, chat_id) for url in urls]
            results = await asyncio.gather(*tasks)
        allowed_all: Set[str] = set()
        foreign_all: Set[str] = set()
        repairs_all: List[tuple[str, str]] = []
        for _, allowed, foreign, repairs in results:
            allowed_all.update(allowed)
            foreign_all.update(foreign)
            repairs_all.extend(repairs)
        state = session_data.setdefault(chat_id, SessionState())
        state.to_send.extend(sorted(allowed_all))
        state.foreign = sorted(foreign_all)
        state.repairs = list(dict.fromkeys(state.repairs + repairs_all))
        await update.message.reply_text(
            f"Добавлено адресов: {len(allowed_all)}. Иностранных доменов: {len(foreign_all)}"
        )
        return


async def ask_include_numeric(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.setdefault(chat_id, SessionState())
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
                [InlineKeyboardButton("↩️ Отмена", callback_data="cancel_include_numeric")],
            ]
        ),
    )


async def include_numeric_emails(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.setdefault(chat_id, SessionState())
    numeric = state.suspect_numeric
    if not numeric:
        await query.answer("Цифровых адресов нет", show_alert=True)
        return
    current = set(state.to_send)
    added = [e for e in numeric if e not in current]
    current.update(numeric)
    state.to_send = sorted(current)
    await query.answer()
    await query.message.reply_text(
        f"➕ Добавлено цифровых адресов: {len(added)}.\nИтого к отправке: {len(state.to_send)}."
    )


async def cancel_include_numeric(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("Ок, цифровые адреса оставлены выключенными.")


async def show_numeric_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.get(chat_id)
    numeric = state.suspect_numeric if state else []
    if not numeric:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(numeric, 60):
        await query.message.reply_text("🔢 Цифровые логины:\n" + "\n".join(chunk))


async def show_foreign_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.get(chat_id)
    foreign = state.foreign if state else []
    if not foreign:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    for chunk in _chunk_list(foreign, 60):
        await query.message.reply_text(
            "🌍 Иностранные домены (исключены):\n" + "\n".join(chunk)
        )


async def apply_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.setdefault(chat_id, SessionState())
    repairs: List[tuple[str, str]] = state.repairs
    if not repairs:
        await query.answer("Нет кандидатов на исправление", show_alert=True)
        return
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
    await query.answer()
    txt = f"🧩 Применено исправлений: {applied}."
    if changed:
        txt += "\n" + "\n".join(changed)
        if applied > len(changed):
            txt += f"\n… и ещё {applied - len(changed)}."
    await query.message.reply_text(txt)


async def show_repairs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.get(chat_id)
    repairs: List[tuple[str, str]] = state.repairs if state else []
    if not repairs:
        await query.answer("Список пуст", show_alert=True)
        return
    await query.answer()
    pairs = [f"{b} → {g}" for (b, g) in repairs]
    for chunk in _chunk_list(pairs, 60):
        await query.message.reply_text(
            "🧩 Возможные исправления:\n" + "\n".join(chunk)
        )


async def send_manual_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    group_code = query.data.split("_")[2]
    template_path = TEMPLATE_MAP[group_code]

    state = session_data.setdefault(chat_id, SessionState())
    emails_raw = state.manual_emails
    all_text = " ".join(emails_raw)
    emails = sorted({normalize_email(x) for x in extract_clean_emails_from_text(all_text)})
    if not emails:
        await query.message.reply_text("❗ Список email пуст.")
        return

    loop = asyncio.get_running_loop()
    recent_sent = await loop.run_in_executor(None, get_recent_6m_union)
    blocked = get_blocked_emails()
    sent_today = get_sent_today()

    to_send = [
        e for e in emails if e not in recent_sent and e not in sent_today and e not in blocked
    ]
    if not to_send:
        await query.message.reply_text(
            "❗ Все адреса уже есть в истории за 6 мес. или в блок-листе."
        )
        state.manual_emails = []
        return

    available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
    if available <= 0 and not is_force_send(chat_id):
        await update.callback_query.message.reply_text(
            f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
            "Если вы исправили ошибки — нажмите «🚀 Игнорировать лимит» и запустите ещё раз."
        )
        return
    if not is_force_send(chat_id) and len(to_send) > available:
        to_send = to_send[:available]
        await query.message.reply_text(
            f"⚠️ Учитываю дневной лимит: будет отправлено {available} адресов из списка."
        )

    await query.message.reply_text(
        f"✉️ Рассылка начата. Отправляем {len(to_send)} писем..."
    )

    sent_count = 0
    errors = []
    for email_addr in to_send:
        try:
            await async_send_email(email_addr, template_path)
            log_sent_email(email_addr, group_code, "ok", chat_id, template_path)
            sent_count += 1
            await asyncio.sleep(1.5)
        except Exception as e:
            errors.append(f"{email_addr} — {e}")
            err = str(e).lower()
            if (
                "invalid mailbox" in err
                or "user is terminated" in err
                or "non-local recipient verification failed" in err
            ):
                add_blocked_email(email_addr)
            log_sent_email(
                email_addr, group_code, "error", chat_id, template_path, str(e)
            )

    await query.message.reply_text(f"✅ Отправлено писем: {sent_count}")
    if errors:
        await query.message.reply_text("Ошибки:\n" + "\n".join(errors))

    state.manual_emails = []
    clear_recent_sent_cache()
    disable_force_send(chat_id)


async def send_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    state = session_data.setdefault(chat_id, SessionState())
    emails = state.to_send
    group_code = state.group
    template_path = state.template
    if not emails or not group_code or not template_path:
        await query.answer("Нет данных для отправки", show_alert=True)
        return

    loop = asyncio.get_running_loop()
    recent_sent = await loop.run_in_executor(None, get_recent_6m_union)
    blocked = get_blocked_emails()
    sent_today = get_sent_today()

    emails_to_send = [
        e for e in emails if e not in recent_sent and e not in sent_today and e not in blocked
    ]
    if not emails_to_send:
        await query.message.reply_text(
            "❗ Все адреса уже есть в истории за 6 мес. или в блок-листе."
        )
        return

    available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
    if available <= 0 and not is_force_send(chat_id):
        await query.message.reply_text(
            f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
            "Если вы исправили ошибки — нажмите «🚀 Игнорировать лимит» и запустите ещё раз."
        )
        return
    if not is_force_send(chat_id) and len(emails_to_send) > available:
        emails_to_send = emails_to_send[:available]
        await query.message.reply_text(
            f"⚠️ Учитываю дневной лимит: будет отправлено {available} адресов из списка."
        )

    await query.message.reply_text(
        f"✉️ Рассылка начата. Отправляем {len(emails_to_send)} писем..."
    )

    sent_count = 0
    bad_emails = []
    for email_addr in emails_to_send:
        try:
            await async_send_email(email_addr, template_path)
            log_sent_email(email_addr, group_code, "ok", chat_id, template_path)
            sent_count += 1
            await asyncio.sleep(1.5)
        except Exception as e:
            error_text = str(e).lower()
            if (
                "invalid mailbox" in error_text
                or "user is terminated" in error_text
                or "non-local recipient verification failed" in error_text
            ):
                add_blocked_email(email_addr)
                bad_emails.append(email_addr)
            log_sent_email(
                email_addr, group_code, "error", chat_id, template_path, str(e)
            )

    await query.message.reply_text(f"✅ Отправлено писем: {sent_count}")
    if bad_emails:
        await query.message.reply_text(
            "🚫 В блок-лист добавлены:\n" + "\n".join(bad_emails)
        )

    clear_recent_sent_cache()
    disable_force_send(chat_id)


async def autosync_imap_with_message(query):
    await query.message.reply_text(
        "🔄 Синхронизация истории отправки с сервером..."
    )
    loop = asyncio.get_running_loop()
    added = await loop.run_in_executor(None, sync_log_with_imap)
    clear_recent_sent_cache()
    await query.message.reply_text(
        f"✅ Синхронизация завершена. В лог добавлено новых адресов: {added}.\n"
        f"История отправки обновлена на последние 6 месяцев."
    )


def _chunk_list(items: List[str], size=60) -> List[List[str]]:
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

