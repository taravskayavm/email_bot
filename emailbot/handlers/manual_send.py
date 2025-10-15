# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import imaplib
import logging
import os
import smtplib
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Set

from asyncio import Lock

from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import ContextTypes
from emailbot.handlers.common import safe_answer

from emailbot.notify import notify

from bot.keyboards import build_templates_kb

from emailbot import config as C
from emailbot import mass_state, messaging
from emailbot import settings as _settings
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
from emailbot.reporting import log_mass_filter_digest
from emailbot.ui.messages import format_dispatch_result, format_error_details
from emailbot.run_control import clear_stop, should_stop
from emailbot.utils import log_error
from emailbot.smtp_client import RobustSMTP

from .preview import (
    go_back as preview_go_back,
    request_edit as preview_request_edit,
    send_preview_report,
)

if TYPE_CHECKING:  # pragma: no cover - import only for type checkers
    import emailbot.bot_handlers as bot_handlers_module

_BOT_HANDLERS_MODULE: Any | None = None


def _bot_handlers() -> "bot_handlers_module":  # type: ignore[name-defined]
    global _BOT_HANDLERS_MODULE
    if _BOT_HANDLERS_MODULE is None:
        from emailbot import bot_handlers as _module

        _BOT_HANDLERS_MODULE = _module
    return _BOT_HANDLERS_MODULE  # type: ignore[return-value]


def _cooldown_api() -> dict[str, Callable[..., object] | None]:
    try:
        from emailbot.cooldown import (
            _merged_history_map as _cooldown_cache_builder,
            is_under_cooldown,
            normalize_email as cooldown_normalize,
        )
    except Exception:
        try:
            from emailbot.cooldown import (
                is_under_cooldown,  # type: ignore
                normalize_email as cooldown_normalize,
            )
        except Exception:
            logger.debug("cooldown API import failed", exc_info=True)
            return {}
        return {
            "is_under_cooldown": is_under_cooldown,
            "normalize_email": cooldown_normalize,
            "build_cache": None,
        }
    return {
        "is_under_cooldown": is_under_cooldown,
        "normalize_email": cooldown_normalize,
        "build_cache": _cooldown_cache_builder,
    }


def _bulk_audit_api() -> Callable[[str], object] | None:
    try:
        from emailbot.audit import start_audit
    except Exception:
        logger.debug("bulk audit API import failed", exc_info=True)
        return None
    return start_audit


def _workers() -> int:
    """Return configured worker count with defensive fallbacks."""

    try:
        value = getattr(_settings, "SEND_MAX_WORKERS", 4)
    except Exception:
        return 4

    try:
        workers = int(value)
        if workers > 0:
            return workers
    except Exception:
        pass

    return 4


def _make_pool() -> ThreadPoolExecutor:
    """Create a thread pool for background tasks with safe defaults."""

    return ThreadPoolExecutor(max_workers=_workers())


def _pick_timeout() -> int:
    """Return SEND_FILE_TIMEOUT value with a sane fallback."""

    try:
        value = getattr(_settings, "SEND_FILE_TIMEOUT", 20)
    except Exception:
        return 20

    try:
        timeout = int(value)
        if timeout > 0:
            return timeout
    except Exception:
        pass

    return 20


FILE_TIMEOUT = _pick_timeout()

logger = logging.getLogger(__name__)


def _get_chat_lock(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> Lock:
    app = getattr(context, "application", None)
    if app is not None:
        locks = app.bot_data.setdefault("locks", {})
    else:
        locks = context.chat_data.setdefault("_locks", {})
    lock = locks.get(chat_id)
    if lock is None:
        lock = Lock()
        locks[chat_id] = lock
    return lock


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu and initialize state."""

    bot_handlers = _bot_handlers()
    bot_handlers.init_state(context)
    keyboard = [
        ["📤 Массовая", "🛑 Стоп", "✉️ Ручная"],
        ["🧹 Очистить список", "📄 Показать исключения"],
        ["🚫 Добавить в исключения", "🧾 О боте"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🔄 Синхронизировать с сервером", "🚀 Игнорировать лимит"],
        ["🔁 Синхронизировать бонсы", "🩺 Диагностика"],
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
        bot_handlers = _bot_handlers()
        session_key = getattr(bot_handlers, "SESSION_KEY", "state")
        state = context.chat_data.get(session_key)
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
    bot_handlers = _bot_handlers()
    data = query.data or ""
    if ":" not in data:
        await query.message.reply_text(
            "⚠️ Некорректный выбор шаблона. Обновите список и попробуйте снова."
        )
        return
    prefix_raw, group_raw = data.split(":", 1)
    prefix = f"{prefix_raw}:"
    template_info = bot_handlers.get_template_from_map(
        context, prefix, group_raw
    )
    template_path_obj = (
        bot_handlers._template_path(template_info)
        if template_info
        else None
    )
    if not template_info or not template_path_obj or not template_path_obj.exists():
        group_code_fallback = bot_handlers._normalize_template_code(group_raw)
        template_info = bot_handlers.get_template(group_code_fallback)
        template_path_obj = bot_handlers._template_path(template_info)
        if not template_info or not template_path_obj or not template_path_obj.exists():
            await query.message.reply_text(
                "⚠️ Шаблон не найден или файл отсутствует. Обновите список и попробуйте снова."
            )
            return
        group_raw = template_info.get("code") or group_code_fallback
    group_code = bot_handlers._normalize_template_code(group_raw)
    template_path = str(template_path_obj)
    template_label = bot_handlers._template_label(template_info)
    if not template_label and group_code:
        template_label = get_template_label(group_code)
    if not template_label:
        template_label = group_code
    state = bot_handlers.get_state(context)
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
    chat_lock = _get_chat_lock(context, chat_id)

    async with chat_lock:
        context.chat_data["preview_source_emails"] = list(emails)
        ready, blocked_foreign, blocked_invalid, skipped_recent, digest = (
            messaging.prepare_mass_mailing(
                emails,
                group_code,
                chat_id=chat_id,
                ignore_cooldown=bool(context.user_data.get("ignore_cooldown")),
            )
        )
        log_mass_filter_digest(
            {
                **digest,
                "batch_id": context.chat_data.get("batch_id"),
                "chat_id": chat_id,
                "entry_url": context.chat_data.get("entry_url"),
            }
        )
        snapshot = bot_handlers._snapshot_mass_digest(
            digest,
            ready_after_cooldown=(
                int(digest.get("ready_after_cooldown"))
                if isinstance(digest, dict)
                and digest.get("ready_after_cooldown") is not None
                else None
            ),
            ready_final=len(ready),
        )
        state.last_digest = snapshot
        state.override_cooldown = bool(context.user_data.get("ignore_cooldown"))
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
                "skipped_duplicates": [],
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


async def send_all(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    **callbacks: Callable[[str], None],
) -> None:
    """Send all prepared e-mails respecting limits."""

    query = update.callback_query
    chat_id = query.message.chat.id
    bot_handlers = _bot_handlers()
    saved = mass_state.load_chat_state(chat_id)
    if saved and saved.get("pending"):
        emails = saved.get("pending", [])
        group_code = saved.get("group")
        template_path = saved.get("template")
        template_label = saved.get("template_label") or ""
    else:
        state = bot_handlers.get_state(context)
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

    chat_lock = _get_chat_lock(context, chat_id)

    async def long_job() -> None:
        audit_start = _bulk_audit_api()
        audit_writer = None
        audit_path: Path | None = None
        cooldown_tools = _cooldown_api()
        build_cache_fn = (
            cooldown_tools.get("build_cache") if cooldown_tools else None
        )
        cooldown_cache = None
        if callable(build_cache_fn):
            try:
                cooldown_cache = build_cache_fn()
            except Exception:
                logger.debug("cooldown cache build failed", exc_info=True)
                cooldown_cache = None

        try:
            cooldown_days = int(getattr(_settings, "SEND_COOLDOWN_DAYS", 180))
        except Exception:
            cooldown_days = 180
        if cooldown_days < 0:
            cooldown_days = 0

        cooldown_meta_cache: Dict[str, str] = {}

        def _cooldown_last_iso(email: str) -> str | None:
            if not cooldown_tools:
                return None
            normalize_fn = cooldown_tools.get("normalize_email")
            lookup_fn = cooldown_tools.get("is_under_cooldown")
            if not callable(lookup_fn):
                return None
            key = email
            if callable(normalize_fn):
                try:
                    key = normalize_fn(email) or email
                except Exception:
                    key = email
            cached = cooldown_meta_cache.get(key)
            if cached is not None:
                return cached or None
            kwargs: Dict[str, object] = {"days": cooldown_days}
            if cooldown_cache is not None:
                kwargs["_cache"] = cooldown_cache
            try:
                _, last_dt = lookup_fn(email, **kwargs)
            except TypeError:
                try:
                    _, last_dt = lookup_fn(email, days=cooldown_days)
                except Exception:
                    last_dt = None
            except Exception:
                logger.debug("cooldown lookup failed for %s", email, exc_info=True)
                last_dt = None
            iso_value = None
            if last_dt:
                try:
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    iso_value = last_dt.astimezone(timezone.utc).isoformat()
                except Exception:
                    iso_value = None
            cooldown_meta_cache[key] = iso_value or ""
            return iso_value

        audit_logged_skip: set[tuple[str, str]] = set()
        batch_id = context.chat_data.get("batch_id")

        def audit_sent(email: str) -> None:
            if not audit_writer:
                return
            try:
                audit_writer.log_sent(email)
            except Exception:
                logger.debug("bulk audit log_sent failed", exc_info=True)

        def audit_skip(
            email: str,
            reason: str,
            meta: Mapping[str, Any] | None = None,
        ) -> None:
            if not email:
                return
            signature = (email, reason)
            if signature in audit_logged_skip:
                return
            audit_logged_skip.add(signature)
            if not audit_writer:
                return
            try:
                audit_writer.log_skip(email, reason, meta=meta)
            except Exception:
                logger.debug("bulk audit log_skip failed", exc_info=True)

        def audit_error(
            email: str,
            reason: str,
            meta: Mapping[str, Any] | None = None,
        ) -> None:
            if not audit_writer:
                return
            try:
                audit_writer.log_error(email, reason, meta=meta)
            except Exception:
                logger.debug("bulk audit log_error failed", exc_info=True)

        def mark_cooldown_skip(email: str) -> None:
            iso_value = _cooldown_last_iso(email)
            meta: Mapping[str, Any] | None = None
            if iso_value:
                meta = {"last_contact": iso_value}
            audit_skip(email, "cooldown_180d", meta)

        async with chat_lock:
            if callable(audit_start):
                label_parts = [display_label or group_code or "manual"]
                if batch_id:
                    label_parts.append(f"batch:{batch_id}")
                label = " | ".join(part for part in label_parts if part)
                try:
                    writer_candidate = audit_start(label)
                except Exception:
                    logger.debug("bulk audit start failed", exc_info=True)
                    writer_candidate = None
                if writer_candidate is not None:
                    audit_writer = writer_candidate
                    audit_path = getattr(writer_candidate, "path", None)

            blocked = get_blocked_emails()
            blocked_norm = {
                messaging._normalize_key(addr)
                for addr in blocked
                if messaging._normalize_key(addr)
            }
            sent_today_norm = get_sent_today()
            preview = context.chat_data.get("send_preview", {}) or {}
            fixed_map: Dict[str, str] = {}
            for item in preview.get("fixed", []):
                if isinstance(item, dict):
                    new_addr = item.get("to")
                    original_addr = item.get("from")
                    if new_addr and original_addr:
                        fixed_map[str(new_addr)] = str(original_addr)

            snapshot = context.chat_data.get("history_snapshot")
            if not isinstance(snapshot, dict):
                snapshot = {}
            frozen_norms = list(snapshot.get("frozen_to_send") or [])
            reason_map = snapshot.get("frozen_reason_map") or {}
            if not isinstance(reason_map, dict):
                reason_map = {}
            frozen_originals = snapshot.get("frozen_original_map") or {}
            if not isinstance(frozen_originals, dict):
                frozen_originals = {}

            saved_state = mass_state.load_chat_state(chat_id)
            if saved_state and saved_state.get("pending"):
                blocked_foreign = list(saved_state.get("blocked_foreign", []))
                blocked_invalid = list(saved_state.get("blocked_invalid", []))
                skipped_recent = list(saved_state.get("skipped_recent", []))
                skipped_duplicates = list(saved_state.get("skipped_duplicates", []))
                sent_ok = list(saved_state.get("sent_ok", []))
                base_candidates = list(saved_state.get("pending", []))
            else:
                state_obj = bot_handlers.get_state(context)
                blocked_foreign = list(state_obj.foreign or [])
                blocked_invalid = []
                skipped_recent = list(state_obj.cooldown_blocked or [])
                skipped_duplicates: List[str] = []
                sent_ok: List[str] = []
                base_candidates = list(state_obj.to_send or emails)

            for addr in blocked_foreign:
                audit_skip(addr, "foreign_domain")
            for addr in blocked_invalid:
                audit_skip(addr, "stop_list")
            for addr in skipped_recent:
                mark_cooldown_skip(addr)
            for addr in skipped_duplicates:
                audit_skip(addr, "duplicate")

            def _order_by_snapshot(candidates: List[str]) -> List[str]:
                if not frozen_norms:
                    return [c for c in candidates if c]

                mapping: Dict[str, List[str]] = {}
                for addr in candidates:
                    norm = messaging._normalize_key(addr)
                    if not norm:
                        continue
                    mapping.setdefault(norm, []).append(addr)

                ordered: List[str] = []
                for norm in frozen_norms:
                    pool = mapping.get(norm)
                    if pool:
                        ordered.append(pool.pop(0))
                    else:
                        original = frozen_originals.get(norm)
                        if original:
                            ordered.append(original)
                return ordered

            ordered_candidates = []
            seen_norm: Set[str] = set()
            for addr in _order_by_snapshot(base_candidates):
                norm = messaging._normalize_key(addr)
                if not norm:
                    continue
                if norm in seen_norm:
                    if addr not in skipped_duplicates:
                        skipped_duplicates.append(addr)
                    audit_skip(addr, "duplicate")
                    continue
                seen_norm.add(norm)
                ordered_candidates.append(addr)

            to_send: List[str] = []
            for addr in ordered_candidates:
                norm = messaging._normalize_key(addr)
                if not norm:
                    continue
                if norm in blocked_norm:
                    if addr not in blocked_invalid:
                        blocked_invalid.append(addr)
                    audit_skip(addr, "stop_list")
                    continue
                if norm in sent_today_norm and not bot_handlers.is_force_send(chat_id):
                    audit_skip(addr, "daily_limit")
                    continue
                if is_foreign(addr):
                    if addr not in blocked_foreign:
                        blocked_foreign.append(addr)
                    audit_skip(addr, "foreign_domain")
                    continue
                if is_suppressed(addr):
                    if addr not in blocked_invalid:
                        blocked_invalid.append(addr)
                    audit_skip(addr, "stop_list")
                    continue
                to_send.append(addr)

            dropped_now: Dict[str, str] = {}
            ready_after_history: List[str] = []
            for addr in to_send:
                norm = messaging._normalize_key(addr)
                skip, skip_reason = messaging._should_skip_by_history(addr)
                if skip:
                    category = reason_map.get(norm)
                    if not category:
                        category = "cooldown" if str(skip_reason or "").startswith("cooldown") else "history"
                    dropped_now[norm] = category
                    actual = frozen_originals.get(norm, addr)
                    if category == "foreign":
                        target = blocked_foreign
                    elif category == "cooldown":
                        target = skipped_recent
                    else:
                        target = blocked_invalid
                    if actual not in target:
                        target.append(actual)
                    reason_name = category or "history"
                    if reason_name == "cooldown":
                        reason_name = "cooldown_180d"
                    elif reason_name == "foreign":
                        reason_name = "foreign_domain"
                    if reason_name == "cooldown_180d":
                        mark_cooldown_skip(actual)
                    else:
                        audit_skip(actual, reason_name)
                else:
                    ready_after_history.append(addr)

            to_send = ready_after_history

            if dropped_now:
                summary: Dict[str, int] = {}
                for cat in dropped_now.values():
                    summary[cat] = summary.get(cat, 0) + 1
                details = "\n".join(
                    f"• {reason}: {count}" for reason, count in sorted(summary.items())
                )
                info_text = "ℹ️ Часть адресов была исключена перед отправкой:\n" + details
                notifier = callbacks.get("on_info") if callbacks else None
                if callable(notifier):
                    try:
                        notifier(info_text)
                    except Exception:
                        pass
                else:
                    try:
                        await context.bot.send_message(chat_id, info_text)
                    except Exception:
                        pass

            if not to_send:
                await query.message.reply_text(
                    "❗ Все адреса уже есть в истории отправок или в блок-листах."
                )
                return

            available = max(0, MAX_EMAILS_PER_DAY - len(sent_today_norm))
            if available <= 0 and not bot_handlers.is_force_send(chat_id):
                logger.info(
                    "Daily limit reached: %s emails sent today (source=sent_log)",
                    len(sent_today_norm),
                )
                await query.message.reply_text(
                    (
                        f"❗ Дневной лимит {MAX_EMAILS_PER_DAY} уже исчерпан.\n"
                        "Если вы исправили ошибки — нажмите "
                        "«🚀 Игнорировать лимит» и запустите ещё раз."
                    )
                )
                return
            if not bot_handlers.is_force_send(chat_id) and len(to_send) > available:
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
                    "skipped_duplicates": skipped_duplicates,
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

        error_details: list[str] = []
        error_addresses: list[str] = []
        cancel_event = context.chat_data.get("cancel_event")
        aborted = False
        smtp = RobustSMTP()
        try:
            while to_send:
                if should_stop():
                    aborted = True
                    break
                if cancel_event and cancel_event.is_set():
                    aborted = True
                    break
                email_addr = to_send.pop(0)
                try:
                    outcome, token, log_key, content_hash = send_email_with_sessions(
                        smtp,
                        imap,
                        sent_folder,
                        email_addr,
                        template_path,
                        subject=messaging.DEFAULT_SUBJECT,
                        fixed_from=fixed_map.get(email_addr),
                        group_title=template_label,
                        group_key=group_code,
                        override_180d=(context.chat_data.get("manual_send_mode") == "all"),
                    )
                    if outcome == SendOutcome.SENT:
                        log_sent_email(
                            email_addr,
                            group_code,
                            "ok",
                            chat_id,
                            template_path,
                            unsubscribe_token=token,
                            key=log_key,
                            subject=messaging.DEFAULT_SUBJECT,
                            content_hash=content_hash,
                        )
                        sent_ok.append(email_addr)
                        audit_sent(email_addr)
                        await asyncio.sleep(1.5)
                    elif outcome == SendOutcome.DUPLICATE:
                        if email_addr not in skipped_duplicates:
                            skipped_duplicates.append(email_addr)
                        audit_skip(email_addr, "duplicate")
                    elif outcome == SendOutcome.COOLDOWN:
                        if email_addr not in skipped_recent:
                            skipped_recent.append(email_addr)
                        mark_cooldown_skip(email_addr)
                    elif outcome == SendOutcome.BLOCKED:
                        if email_addr not in blocked_invalid:
                            blocked_invalid.append(email_addr)
                        audit_skip(email_addr, "stop_list")
                    elif outcome == SendOutcome.ERROR:
                        if email_addr not in error_addresses:
                            error_addresses.append(email_addr)
                        detail = "ошибка при отправке"
                        error_details.append(detail)
                        audit_error(email_addr, "send_error", {"detail": detail})
                    else:
                        if email_addr not in error_addresses:
                            error_addresses.append(email_addr)
                        detail = f"непредвиденный исход: {outcome}"
                        error_details.append(detail)
                        audit_error(email_addr, "unexpected_outcome", {"detail": detail})
                except smtplib.SMTPResponseException as e:
                    code = int(getattr(e, "smtp_code", 0) or 0)
                    raw = getattr(e, "smtp_error", b"") or b""
                    if isinstance(raw, (bytes, bytearray)):
                        msg = raw.decode("utf-8", "ignore")
                    else:
                        msg = str(raw)
                    code_text = f"{code} " if code else ""
                    detail = f"SMTP {code_text}{msg}".strip()
                    error_details.append(detail)
                    add_bounce(email_addr, code, msg, phase="send")
                    if is_hard_bounce(code, msg):
                        suppress_add(email_addr, code, "hard bounce on send")
                    target_list = (
                        blocked_invalid if is_hard_bounce(code, msg) else error_addresses
                    )
                    if email_addr not in target_list:
                        target_list.append(email_addr)
                    reason = f"smtp_error:{code}" if code else "smtp_error"
                    audit_error(
                        email_addr,
                        reason,
                        {"code": code, "message": msg},
                    )
                except Exception as e:
                    error_details.append(str(e))
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
                    reason = f"smtp_error:{code}" if code else "smtp_error"
                    audit_error(
                        email_addr,
                        reason,
                        {"code": code, "message": msg or str(e)},
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
                        "skipped_duplicates": skipped_duplicates,
                    },
                )
        finally:
            smtp.close()
        imap.logout()
        if not to_send:
            mass_state.clear_chat_state(chat_id)

        total_sent = len(sent_ok)
        total_skipped = len(skipped_recent)
        total_blocked = len(blocked_foreign) + len(blocked_invalid)
        total_duplicates = len(skipped_duplicates)
        total = total_sent + total_skipped + total_blocked + total_duplicates
        report_text = format_dispatch_result(
            total,
            total_sent,
            total_skipped,
            total_blocked,
            total_duplicates,
            aborted=aborted,
        )
        filtered_lines = []
        for line in report_text.splitlines():
            if line.startswith("⏳") and total_skipped == 0:
                continue
            if line.startswith("🚫") and total_blocked == 0:
                continue
            filtered_lines.append(line)
        report_text = "\n".join(filtered_lines)
        if blocked_foreign:
            report_text += f"\n🌍 Иностранные домены (отложены): {len(blocked_foreign)}"
        if blocked_invalid:
            report_text += f"\n🚫 Недоставляемые/в блок-листе: {len(blocked_invalid)}"
        if error_addresses:
            report_text = (
                f"{report_text}\n❌ Ошибок при отправке: {len(error_addresses)}"
                if report_text
                else f"❌ Ошибок при отправке: {len(error_addresses)}"
            )
        if audit_path and audit_writer and getattr(audit_writer, "enabled", False):
            report_text = f"{report_text}\n\n📄 Аудит: {audit_path}"

        # Безопасная отправка: notify сам разрежет текст на куски < 4096
        await notify(query.message, report_text, event="finish")
        if error_details:
            error_report = format_error_details(error_details)
            if error_report:
                await notify(query.message, error_report, event="error")

        clear_recent_sent_cache()
        bot_handlers.disable_force_send(chat_id)

    clear_stop()
    messaging.create_task_with_logging(
        long_job(),
        query.message.reply_text,
        task_name="manual_mass_send",
    )


async def handle_send_flow_actions(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Route inline keyboard actions shown before bulk send."""

    query = update.callback_query
    data = (query.data or "").strip()
    if data == "bulk:send:start":
        await send_all(update, context)
        return
    if data == "bulk:send:back":
        await preview_go_back(update, context)
        return
    if data == "bulk:send:edit":
        await preview_request_edit(update, context)
        return
    await safe_answer(query)
