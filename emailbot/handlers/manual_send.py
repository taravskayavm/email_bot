# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import imaplib
import json
import logging
import os
import smtplib
import time
from pathlib import Path
from typing import Dict, List, Set

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import ContextTypes
from emailbot.handlers.common import safe_answer

from emailbot.notify import notify

from bot.keyboards import build_templates_kb

from emailbot import mass_state, messaging
from emailbot.extraction import normalize_email
from emailbot.messaging import (
    MAX_EMAILS_PER_DAY,
    SendOutcome,
    clear_recent_sent_cache,
    get_blocked_emails,
    get_preferred_sent_folder,
    get_sent_today,
    log_sent_email,
    send_email_with_sessions,
)
from emailbot.messaging_utils import (
    add_bounce,
    is_foreign,
    is_hard_bounce,
    is_suppressed,
    suppress_add,
)
from emailbot import history_service
from emailbot.reporting import build_mass_report_text, log_mass_filter_digest
from emailbot.utils import log_error
from utils.smtp_client import RobustSMTP

import emailbot.bot_handlers as bot_handlers_module
from .preview import send_preview_report

logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu and initialize state."""

    bot_handlers_module.init_state(context)
    keyboard = [
        ["📤 Массовая", "🛑 Стоп", "✉️ Ручная"],
        ["🧹 Очистить список", "📄 Показать исключения"],
        ["🚫 Добавить в исключения", "🧾 О боте"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🔄 Синхронизировать с сервером", "🚀 Игнорировать лимит"],
        ["🔁 Синхронизировать бонсы"],
    ]
    markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("Можно загрузить данные", reply_markup=markup)


async def manual_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Выбор режима отправки в ручной рассылке."""

    query = update.callback_query
    await safe_answer(query)
    data = query.data or ""
    context.chat_data["manual_send_mode"] = (
        "allowed" if data.endswith("allowed") else "all"
    )
    await query.message.reply_text(
        "Режим установлен: "
        + (
            "только разрешённым ✅"
            if data.endswith("allowed")
            else "всем (игнорировать 180 дней) ⚠️"
        )
    )


async def proceed_to_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Switch to the mailing group selection step."""

    query = update.callback_query
    # Сначала мгновенно отвечаем на нажатие, чтобы не словить TTL
    await safe_answer(query)
    current = context.chat_data.get("current_template_code")
    if not current:
        state = context.chat_data.get(bot_handlers_module.SESSION_KEY)
        if state and getattr(state, "group", None):
            current = state.group
    await query.message.reply_text(
        "⬇️ Выберите направление рассылки:",
        reply_markup=build_templates_kb(
            context, current_code=current
        ),
    )


async def select_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle group selection and prepare messages for sending."""

    query = update.callback_query
    await safe_answer(query)
    data = query.data or ""
    if ":" not in data:
        await query.message.reply_text(
            "⚠️ Некорректный выбор шаблона. Обновите список и попробуйте снова."
        )
        return
    prefix_raw, group_raw = data.split(":", 1)
    prefix = f"{prefix_raw}:"
    template_info = bot_handlers_module.get_template_from_map(
        context, prefix, group_raw
    )
    template_path_obj = (
        bot_handlers_module._template_path(template_info)
        if template_info
        else None
    )
    if not template_info or not template_path_obj or not template_path_obj.exists():
        group_code_fallback = bot_handlers_module._normalize_template_code(group_raw)
        template_info = bot_handlers_module.get_template(group_code_fallback)
        template_path_obj = bot_handlers_module._template_path(template_info)
        if not template_info or not template_path_obj or not template_path_obj.exists():
            await query.message.reply_text(
                "⚠️ Шаблон не найден или файл отсутствует. Обновите список и попробуйте снова."
            )
            return
        group_raw = template_info.get("code") or group_code_fallback
    group_code = bot_handlers_module._normalize_template_code(group_raw)
    template_path = str(template_path_obj)
    template_label = bot_handlers_module._template_label(template_info)
    if not template_label and group_code:
        template_label = get_template_label(group_code)
    if not template_label:
        template_label = group_code
    state = bot_handlers_module.get_state(context)
    emails = state.to_send
    state.group = group_code
    state.template = template_path
    state.template_label = template_label
    context.chat_data["current_template_code"] = group_code
    context.chat_data["current_template_label"] = template_label
    context.chat_data["current_template_path"] = template_path
    try:
        await query.message.edit_reply_markup(
            reply_markup=build_templates_kb(
                context, current_code=group_code, prefix=prefix
            )
        )
    except Exception:
        pass
    # Короткое подтверждение без раскрытия пути к файлу (см. EBOT-0918-02)
    await query.message.reply_text(f"✅ Выбран шаблон: «{template_label}»")
    chat_id = query.message.chat.id
    context.chat_data["preview_source_emails"] = list(emails)
    ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
        messaging.prepare_mass_mailing(emails, group_code, chat_id=chat_id)
    )
    log_mass_filter_digest(
        {
            **digest,
            "batch_id": context.chat_data.get("batch_id"),
            "chat_id": chat_id,
            "entry_url": context.chat_data.get("entry_url"),
        }
    )
    state.to_send = ready
    mass_state.save_chat_state(
        chat_id,
        {
            "group": group_code,
            "template": template_path,
            "template_label": template_label,
            "pending": ready,
            "blocked_foreign": blocked_foreign,
            "blocked_invalid": blocked_invalid,
            "skipped_recent": skipped_recent,
            "batch_id": context.chat_data.get("batch_id"),
        },
    )
    if not ready:
        await query.message.reply_text(
            "Все адреса уже в истории за 180 дней или в блок-листах.",
            reply_markup=None,
        )
        return
    await send_preview_report(
        update,
        context,
        group_code,
        template_label,
        ready,
        blocked_foreign,
        blocked_invalid,
        skipped_recent,
    )


async def send_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send all prepared e-mails respecting limits."""

    query = update.callback_query
    chat_id = query.message.chat.id
    saved = mass_state.load_chat_state(chat_id)
    if saved and saved.get("pending"):
        emails = saved.get("pending", [])
        group_code = saved.get("group")
        template_path = saved.get("template")
        template_label = saved.get("template_label") or ""
    else:
        state = bot_handlers_module.get_state(context)
        emails = state.to_send
        group_code = state.group
        template_path = state.template
        template_label = state.template_label or ""
    if not emails or not group_code or not template_path:
        await safe_answer(query, text="Нет данных для отправки", show_alert=True)
        return
    if not Path(template_path).exists():
        await safe_answer(query, text="Шаблон недоступен", show_alert=True)
        await query.message.reply_text(
            "⚠️ Шаблон не найден или файл отсутствует. Нажмите «🧭 Сменить группу» и выберите шаблон."
        )
        return
    await safe_answer(query)
    group_code = str(group_code)
    template_label = str(template_label or "")
    if not template_label and group_code:
        template_label = get_template_label(group_code)
    if not template_label and group_code:
        template_label = group_code
    display_label = template_label or group_code
    if template_label and template_label.lower() != group_code.lower():
        display_label = f"{template_label} ({group_code})"
    await query.message.reply_text(
        "Запущено — выполняю в фоне...\n" f"Шаблон: {display_label}"
    )

    async def long_job() -> None:
        lookup_days = history_service.get_days_rule_default()
        blocked = get_blocked_emails()
        sent_today = get_sent_today()
        preview = context.chat_data.get("send_preview", {}) or {}
        fixed_map: Dict[str, str] = {}
        for item in preview.get("fixed", []):
            if isinstance(item, dict):
                new_addr = item.get("to")
                original_addr = item.get("from")
                if new_addr and original_addr:
                    fixed_map[str(new_addr)] = str(original_addr)

        saved_state = mass_state.load_chat_state(chat_id)
        if saved_state and saved_state.get("pending"):
            blocked_foreign = saved_state.get("blocked_foreign", [])
            blocked_invalid = saved_state.get("blocked_invalid", [])
            skipped_recent = saved_state.get("skipped_recent", [])
            sent_ok = saved_state.get("sent_ok", [])
            to_send = saved_state.get("pending", [])
        else:
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

            try:
                allowed, rejected = history_service.filter_by_days(
                    queue, group_code or "", lookup_days
                )
            except Exception:  # pragma: no cover - defensive fallback
                allowed = list(queue)
                rejected = []
            to_send = list(allowed)
            skipped_recent.extend(rejected)

            deduped: List[str] = []
            seen_norm: Set[str] = set()
            dup_skipped = 0
            for e in to_send:
                norm = normalize_email(e)
                if norm in seen_norm:
                    dup_skipped += 1
                else:
                    seen_norm.add(norm)
                    deduped.append(e)
            to_send = deduped

            log_mass_filter_digest(
                {
                    "input_total": len(emails),
                    "after_suppress": len(queue),
                    "foreign_blocked": len(blocked_foreign),
                    "after_180d": len(to_send),
                    "sent_planned": len(to_send),
                    "skipped_by_dup_in_batch": dup_skipped,
                }
            )

            mass_state.save_chat_state(
                chat_id,
                {
                    "group": group_code,
                    "template": template_path,
                    "template_label": template_label,
                    "pending": to_send,
                    "sent_ok": sent_ok,
                    "blocked_foreign": blocked_foreign,
                    "blocked_invalid": blocked_invalid,
                    "skipped_recent": skipped_recent,
                },
            )

        if not to_send:
            await query.message.reply_text(
                "❗ Все адреса уже есть в истории отправок или в блок-листах."
            )
            return

        available = max(0, MAX_EMAILS_PER_DAY - len(sent_today))
        if available <= 0 and not bot_handlers_module.is_force_send(chat_id):
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
        if not bot_handlers_module.is_force_send(chat_id) and len(to_send) > available:
            to_send = to_send[:available]
            await query.message.reply_text(
                (
                    f"⚠️ Учитываю дневной лимит: будет отправлено "
                    f"{available} адресов из списка."
                )
            )
            mass_state.save_chat_state(
                chat_id,
                {
                    "group": group_code,
                    "template": template_path,
                    "template_label": template_label,
                    "pending": to_send,
                    "sent_ok": sent_ok,
                    "blocked_foreign": blocked_foreign,
                    "blocked_invalid": blocked_invalid,
                    "skipped_recent": skipped_recent,
                },
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

        batch_id = context.chat_data.get("batch_id")
        audit_path = Path("var") / f"bulk_audit_{batch_id or int(time.time())}.jsonl"
        try:
            audit_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            logger.debug("bulk audit mkdir failed", exc_info=True)

        def _audit(email: str, status: str, detail: str = "") -> None:
            try:
                with audit_path.open("a", encoding="utf-8") as f:
                    f.write(
                        json.dumps(
                            {
                                "email": email,
                                "status": status,
                                "detail": detail,
                                "ts": time.time(),
                            }
                        )
                        + "\n"
                    )
            except Exception:
                logger.debug("bulk audit append failed", exc_info=True)

        errors: list[str] = []
        error_addresses: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        smtp = RobustSMTP()
        try:
            while to_send:
                if cancel_event and cancel_event.is_set():
                    break
                email_addr = to_send.pop(0)
                try:
                    outcome, token = send_email_with_sessions(
                        smtp,
                        imap,
                        sent_folder,
                        email_addr,
                        template_path,
                        fixed_from=fixed_map.get(email_addr),
                        group_title=template_label,
                        group_key=group_code,
                    )
                    if outcome == SendOutcome.SENT:
                        log_sent_email(
                            email_addr,
                            group_code,
                            "ok",
                            chat_id,
                            template_path,
                            unsubscribe_token=token,
                        )
                        sent_ok.append(email_addr)
                        _audit(email_addr, "sent")
                        await asyncio.sleep(1.5)
                    elif outcome == SendOutcome.COOLDOWN:
                        if email_addr not in skipped_recent:
                            skipped_recent.append(email_addr)
                        _audit(email_addr, "cooldown")
                    elif outcome == SendOutcome.BLOCKED:
                        if email_addr not in blocked_invalid:
                            blocked_invalid.append(email_addr)
                        _audit(email_addr, "blocked")
                    elif outcome == SendOutcome.ERROR:
                        if email_addr not in error_addresses:
                            error_addresses.append(email_addr)
                        detail = "send outcome error"
                        errors.append(f"{email_addr} — {detail}")
                        _audit(email_addr, "error", detail)
                    else:
                        if email_addr not in error_addresses:
                            error_addresses.append(email_addr)
                        detail = f"outcome {outcome}"
                        errors.append(f"{email_addr} — {detail}")
                        _audit(email_addr, "error", detail)
                except smtplib.SMTPResponseException as e:
                    code = int(getattr(e, "smtp_code", 0) or 0)
                    raw = getattr(e, "smtp_error", b"") or b""
                    if isinstance(raw, (bytes, bytearray)):
                        msg = raw.decode("utf-8", "ignore")
                    else:
                        msg = str(raw)
                    detail = f"{code} {msg}".strip()
                    errors.append(f"{email_addr} — {detail}")
                    add_bounce(email_addr, code, msg, phase="send")
                    if is_hard_bounce(code, msg):
                        suppress_add(email_addr, code, "hard bounce on send")
                    target_list = (
                        blocked_invalid if is_hard_bounce(code, msg) else error_addresses
                    )
                    if email_addr not in target_list:
                        target_list.append(email_addr)
                    _audit(email_addr, "error", detail)
                except Exception as e:
                    errors.append(f"{email_addr} — {e}")
                    code = None
                    msg = None
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
                    if is_hard_bounce(code, msg):
                        suppress_add(email_addr, code, "hard bounce on send")
                    if email_addr not in error_addresses:
                        error_addresses.append(email_addr)
                    _audit(email_addr, "error", str(e))
                mass_state.save_chat_state(
                    chat_id,
                    {
                        "group": group_code,
                        "template": template_path,
                        "template_label": template_label,
                        "pending": to_send,
                        "sent_ok": sent_ok,
                        "blocked_foreign": blocked_foreign,
                        "blocked_invalid": blocked_invalid,
                        "skipped_recent": skipped_recent,
                    },
                )
        finally:
            smtp.close()
        imap.logout()
        if not to_send:
            mass_state.clear_chat_state(chat_id)

        report_text = build_mass_report_text(
            sent_ok,
            skipped_recent,
            blocked_foreign,
            blocked_invalid,
        )
        if not skipped_recent:
            report_text = report_text.replace(
                "\n⏳ Пропущены (<180 дней/идемпотентность): 0", ""
            )
            if report_text.startswith("⏳ Пропущены (<180 дней/идемпотентность): 0\n"):
                report_text = report_text.split("\n", 1)[-1]
        if "🚫 В блок-листе/недоступны: 0" in report_text:
            report_text = report_text.replace(
                "\n🚫 В блок-листе/недоступны: 0", ""
            )
            if report_text.startswith("🚫 В блок-листе/недоступны: 0\n"):
                report_text = report_text.split("\n", 1)[-1]
        if error_addresses:
            report_text = (
                f"{report_text}\n❌ Ошибок при отправке: {len(error_addresses)}"
                if report_text
                else f"❌ Ошибок при отправке: {len(error_addresses)}"
            )
        if audit_path:
            report_text = f"{report_text}\n\n📄 Аудит: {audit_path}"

        # Безопасная отправка: notify сам разрежет текст на куски < 4096
        await notify(query.message, report_text, event="finish")
        if errors:
            await notify(
                query.message,
                "Ошибки:\n" + "\n".join(errors),
                event="error",
            )

        clear_recent_sent_cache()
        bot_handlers_module.disable_force_send(chat_id)

    messaging.create_task_with_logging(long_job(), query.message.reply_text)
