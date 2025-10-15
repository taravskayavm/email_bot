# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, TYPE_CHECKING

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from emailbot.handlers.common import safe_answer
from emailbot.notify import notify
from services.templates import get_template_label

from emailbot import config as C
from emailbot import extraction as extraction_module
from emailbot import history_service, mass_state, messaging
from emailbot.dedupe_global import build_hits_with_sources, dedupe_across_sources
from emailbot.edit_service import (
    apply_edits as apply_saved_edits,
    clear_edits as clear_saved_edits,
    list_edits as list_saved_edits,
    save_edit as save_edit_record,
)
from emailbot.report_preview import PreviewData, build_preview_workbook
from emailbot.utils_preview_export import build_preview_excel
from bot.keyboards import send_flow_keyboard
from emailbot.ui.messages import format_dispatch_preview
from emailbot.extraction import normalize_email
from utils.email_clean import (
    dedupe_keep_original,
    drop_leading_char_twins,
    parse_emails_unified,
)
from emailbot.parallel_parse import parallel_map_files

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from emailbot.bot_handlers import SessionState

logger = logging.getLogger(__name__)

PREVIEW_DIR = Path("var")
_REFRESH_PREFIX = "preview_refresh:"

MAX_TG = 4096
_PARAGRAPH_CHUNK = 3000


def _extract_emails_from_files(files: Sequence[str]) -> list[str]:
    if not files:
        return []

    def _worker(path: str) -> list[str]:
        try:
            emails, _ = extraction_module.extract_any(path)
            return list(emails)
        except Exception:
            return []

    chunks = parallel_map_files(files, _worker)
    combined: list[str] = []
    for chunk in chunks:
        if not chunk:
            continue
        combined.extend(chunk)
    if not combined:
        return []
    seen: set[str] = set()
    unique: list[str] = []
    for item in combined:
        candidate = str(item).strip()
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _split_for_telegram(text: str) -> list[str]:
    parts: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        if not block:
            candidate = current + ("\n\n" if current else "")
            if len(candidate) <= MAX_TG:
                current = candidate
            else:
                if current:
                    parts.append(current)
                current = ""
            continue
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= MAX_TG:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        if len(block) <= MAX_TG:
            current = block
            continue
        start = 0
        while start < len(block):
            chunk = block[start : start + _PARAGRAPH_CHUNK]
            parts.append(chunk)
            start += _PARAGRAPH_CHUNK
    if current:
        parts.append(current)
    return [part for part in parts if part]


async def _safe_reply_text(message, text: str, **kwargs):
    if not text:
        return
    if len(text) <= MAX_TG:
        await message.reply_text(text, **kwargs)
        return
    chunks = _split_for_telegram(text)
    if not chunks:
        return
    first, *rest = chunks
    await message.reply_text(first, **kwargs)
    for part in rest:
        await message.reply_text(part)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_dt(dt: datetime | None) -> str:
    value = _ensure_utc(dt)
    return value.isoformat() if value else ""


def _days_left(last: datetime | None, rule_days: int) -> int:
    value = _ensure_utc(last)
    if value is None:
        return 0
    delta = _utc_now() - value
    return max(0, rule_days - delta.days)


def _fixed_map(chat_preview: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    fixed_items = chat_preview.get("fixed") if isinstance(chat_preview, dict) else None
    if not isinstance(fixed_items, Iterable):
        return mapping
    for item in fixed_items:
        if not isinstance(item, dict):
            continue
        new_addr = str(item.get("to") or "").strip()
        original = str(item.get("from") or "").strip()
        if new_addr:
            mapping[new_addr] = original
    return mapping


def _collect_valid(
    emails: Sequence[str],
    group: str,
    fixed_map: dict[str, str],
    rule_days: int,
    source_map: Mapping[str, Sequence[str]] | None = None,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for email in emails:
        if email in seen:
            continue
        seen.add(email)
        last = history_service.get_last_sent(email, group)
        reason_parts: list[str] = []
        if email in fixed_map:
            reason_parts.append(f"fixed:{fixed_map[email]}")
        if last is None:
            reason_parts.append("new")
        else:
            left = _days_left(last, rule_days)
            if left > 0:
                reason_parts.append(f"override:{left}d")
            else:
                reason_parts.append("ok")
        norm = normalize_email(email) or email.strip().lower()
        sources = []
        if source_map and norm:
            raw_sources = source_map.get(norm)
            if raw_sources:
                sources = [str(item) for item in raw_sources if item]
        rows.append(
            {
                "email": email,
                "last_sent_at": _format_dt(last),
                "reason": ", ".join(reason_parts),
                "source": sources[0] if sources else "",
            }
        )
    rows.sort(key=lambda row: row.get("email", ""))
    return rows


def _collect_rejected(
    emails: Iterable[str], group: str, rule_days: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for email in emails:
        if email in seen:
            continue
        seen.add(email)
        last = history_service.get_last_sent(email, group)
        rows.append(
            {
                "email": email,
                "last_sent_at": _format_dt(last),
                "days_left": _days_left(last, rule_days),
            }
        )
    rows.sort(key=lambda row: row.get("email", ""))
    return rows


def _collect_suspicious(state: SessionState | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not state:
        return rows
    seen: set[str] = set()
    dropped = getattr(state, "dropped", []) or []
    for item in dropped:
        if not isinstance(item, (tuple, list)) or len(item) < 2:
            continue
        email = str(item[0])
        reason = str(item[1])
        if email in seen:
            continue
        seen.add(email)
        rows.append({"email": email, "reason": reason})
    rows.sort(key=lambda row: row.get("email", ""))
    return rows


def _collect_blocked(
    blocked_foreign: Sequence[str], blocked_invalid: Sequence[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for email in blocked_invalid:
        if email in seen:
            continue
        seen.add(email)
        rows.append({"email": email, "source": "suppress-list"})
    for email in blocked_foreign:
        if email in seen:
            continue
        seen.add(email)
        rows.append({"email": email, "source": "foreign-domain"})
    rows.sort(key=lambda row: row.get("email", ""))
    return rows


def _normalise_sources(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value if item)
    return value


def _collect_duplicates(context: ContextTypes.DEFAULT_TYPE) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    raw_candidates: list[Any] = []
    for key in ("preview_duplicates", "duplicates", "duplicates_preview"):
        value = context.chat_data.get(key)
        if not value:
            continue
        if isinstance(value, list):
            raw_candidates.extend(value)
        else:
            raw_candidates.append(value)
    for item in raw_candidates:
        if not isinstance(item, dict):
            continue
        email = str(item.get("email") or "").strip()
        if not email:
            continue
        rows.append(
            {
                "email": email,
                "occurrences": item.get("occurrences"),
                "source_files": _normalise_sources(item.get("source_files")),
            }
        )
    rows.sort(key=lambda row: row.get("email", ""))
    return rows


def _get_state(context: ContextTypes.DEFAULT_TYPE) -> SessionState | None:
    from emailbot import bot_handlers as bot_handlers_module  # local import to avoid cycles

    key = bot_handlers_module.SESSION_KEY
    state = context.chat_data.get(key)
    return state if isinstance(state, bot_handlers_module.SessionState) else state


def _build_preview_data(
    context: ContextTypes.DEFAULT_TYPE,
    group_code: str,
    group_label: str,
    ready: Sequence[str],
    blocked_foreign: Sequence[str],
    blocked_invalid: Sequence[str],
    skipped_recent: Sequence[str],
    rule_days: int,
) -> PreviewData:
    state = _get_state(context)
    preview_chat = context.chat_data.get("send_preview") or {}
    fixed_map = _fixed_map(preview_chat if isinstance(preview_chat, dict) else {})
    raw_sources = context.chat_data.get("preview_source_map")
    source_map: Mapping[str, Sequence[str]] | None = None
    if isinstance(raw_sources, dict):
        source_map = {str(k): list(v) if isinstance(v, (list, tuple)) else [v] for k, v in raw_sources.items()}
    valid_rows = _collect_valid(ready, group_code, fixed_map, rule_days, source_map)
    rejected_rows = _collect_rejected(skipped_recent, group_code, rule_days)
    suspicious_rows = _collect_suspicious(state)
    blocked_rows = _collect_blocked(blocked_foreign, blocked_invalid)
    duplicates_rows = _collect_duplicates(context)
    group_name = group_label or group_code or getattr(state, "group", "") or ""
    return PreviewData(
        group=group_name,
        valid=valid_rows,
        rejected_180d=rejected_rows,
        suspicious=suspicious_rows,
        blocked=blocked_rows,
        duplicates=duplicates_rows,
    )


def _compose_caption(data: PreviewData, rule_days: int, filename: str) -> str:
    base = format_dispatch_preview(
        {
            "ready_to_send": len(data.valid),
            "deferred_180d": len(data.rejected_180d),
            "in_blacklists": len(data.blocked),
            "need_review": len(data.suspicious),
        },
        xlsx_name=filename,
    )
    if rule_days != 180 and data.rejected_180d:
        base = base.replace("180 дн.", f"{rule_days} дн.")
    if data.duplicates:
        base += f"\n🔁 Возможные дубликаты: {len(data.duplicates)}"
    return base


def _preview_keyboard() -> InlineKeyboardMarkup:
    return send_flow_keyboard()


async def send_preview_report(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    group_code: str,
    group_label: str,
    ready: Sequence[str],
    blocked_foreign: Sequence[str],
    blocked_invalid: Sequence[str],
    skipped_recent: Sequence[str],
) -> None:
    """Generate an XLSX preview report and send it to the user."""

    rule_days = history_service.get_days_rule_default()
    data = _build_preview_data(
        context,
        group_code,
        group_label,
        ready,
        blocked_foreign,
        blocked_invalid,
        skipped_recent,
        rule_days,
    )
    chat = update.effective_chat
    chat_id = chat.id if chat else 0
    path = PREVIEW_DIR / f"preview_{chat_id}.xlsx"
    file_path = path
    try:
        build_preview_workbook(data, path)
    except Exception:  # pragma: no cover - fallback for optional deps
        logger.exception("Failed to build detailed preview workbook; using fallback export.")
        fallback_path = build_preview_excel(
            (row.get("email", "") for row in data.valid),
            (row.get("email", "") for row in data.suspicious),
        )
        file_path = Path(fallback_path)
    caption = _compose_caption(data, rule_days, file_path.name)
    keyboard = _preview_keyboard()
    with file_path.open("rb") as fh:
        await update.callback_query.message.reply_document(
            document=fh,
            filename=file_path.name,
            caption=caption,
            reply_markup=keyboard,
        )


async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the "Вернуться / Править" button press."""

    query = update.callback_query
    await safe_answer(query, cache_time=0)
    preview = context.chat_data.get("send_preview") or {}
    dropped = []
    cooldown_blocked: list[str] = []
    if isinstance(preview, dict):
        dropped = preview.get("dropped", []) or []
        raw_cooldown = preview.get("cooldown_blocked") or []
        if isinstance(raw_cooldown, list):
            cooldown_blocked = [
                str(item).strip() for item in raw_cooldown if str(item).strip()
            ]
    lines = ["Можно вернуться к редактированию списка."]
    if C.ALLOW_EDIT_AT_PREVIEW:
        lines.append(
            "Используйте кнопку «✏️ Исправить адрес» в сообщении с анализом выше."
        )
    else:
        lines.append(
            "После выбора направления будет доступна кнопка «✏️ Исправить адрес»."
        )
    if dropped:
        preview_lines = [
            "",
            "⚠️ Текущие подозрительные:",
            *(
                f"{idx + 1}) {addr} — {reason}" for idx, (addr, reason) in enumerate(dropped[:10])
            ),
        ]
        lines.extend(preview_lines)
    if cooldown_blocked:
        sample = cooldown_blocked[: min(50, len(cooldown_blocked))]
        lines.append("")
        lines.append("🕒 Под кулдауном (180 дней):")
        lines.extend(sample)
        if len(cooldown_blocked) > len(sample):
            lines.append(f"… и ещё {len(cooldown_blocked) - len(sample)}")
    await notify(query.message, "\n".join(lines), event="analysis", force=True)


def _format_edit_ts(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return value
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")


def _get_source_emails(context: ContextTypes.DEFAULT_TYPE) -> list[str]:
    stored = context.chat_data.get("preview_source_emails")
    if isinstance(stored, list):
        return list(stored)
    files_ref = context.chat_data.get("preview_source_files")
    if isinstance(files_ref, list) and files_ref:
        parsed = _extract_emails_from_files([str(item) for item in files_ref if str(item)])
        if parsed:
            return parsed
    preview = context.chat_data.get("send_preview")
    if isinstance(preview, dict):
        final = preview.get("final")
        if isinstance(final, list):
            return list(final)
    state = _get_state(context)
    if state and getattr(state, "to_send", None):
        return list(state.to_send)
    return []


async def request_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Prompt the user to enter an address correction."""

    query = update.callback_query
    # быстрый ack, чтобы не протухал callback id даже если обработка долгая
    await safe_answer(query, text="⏳ Обрабатываю…", cache_time=0)
    context.chat_data["preview_edit_pending"] = True
    await _safe_reply_text(query.message,
        (
            "Введите правку в формате «старый -> новый».\n"
            "Пример: old@example.ru -> new@example.ru"
        )
    )


async def show_edits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the list of saved edits for the chat."""

    query = update.callback_query
    await safe_answer(query, cache_time=0)
    chat = update.effective_chat
    chat_id = chat.id if chat else 0
    rows = list_saved_edits(chat_id)
    if not rows:
        await _safe_reply_text(query.message, "📄 Сохранённых правок нет.")
        return
    limit = 20
    lines = ["📄 Текущие правки:"]
    for idx, (old_email, new_email, edited_at) in enumerate(rows[:limit], start=1):
        ts = _format_edit_ts(edited_at)
        lines.append(f"{idx}) {old_email} → {new_email} ({ts})")
    if len(rows) > limit:
        lines.append(f"… и ещё {len(rows) - limit}.")
    await _safe_reply_text(query.message, "\n".join(lines))


async def reset_edits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear all saved edits for the current chat."""

    query = update.callback_query
    await safe_answer(query, cache_time=0)
    chat = update.effective_chat
    chat_id = chat.id if chat else 0
    clear_saved_edits(chat_id)
    preview = context.chat_data.get("send_preview")
    if isinstance(preview, dict):
        preview["fixed"] = []
        context.chat_data["send_preview"] = preview
    await _safe_reply_text(query.message, "♻️ Правки удалены.")


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Validate and store the edit provided by the user."""

    text = update.message.text or ""
    raw = text.strip()
    normalized = raw.replace("→", "->")
    if "->" not in normalized:
        context.chat_data["preview_edit_pending"] = True
        await _safe_reply_text(update.message, "❌ Формат: старый -> новый")
        return
    old_raw, new_raw = (part.strip() for part in normalized.split("->", 1))
    if not old_raw or not new_raw:
        context.chat_data["preview_edit_pending"] = True
        await _safe_reply_text(update.message, "❌ Укажите адреса в формате: старый -> новый")
        return
    if "@" not in old_raw:
        context.chat_data["preview_edit_pending"] = True
        await _safe_reply_text(update.message, "❌ Старый адрес должен содержать символ @.")
        return

    parsed = parse_emails_unified(new_raw)
    parsed = dedupe_keep_original(parsed)
    parsed = drop_leading_char_twins(parsed)
    if not parsed:
        context.chat_data["preview_edit_pending"] = True
        await _safe_reply_text(update.message, "❌ Не удалось распознать новый адрес.")
        return
    if len(parsed) > 1:
        context.chat_data["preview_edit_pending"] = True
        await _safe_reply_text(update.message, "❌ Укажите только один новый адрес.")
        return

    new_email = parsed[0]
    chat = update.effective_chat
    chat_id = chat.id if chat else 0
    save_edit_record(chat_id, old_raw, new_email)
    context.chat_data["preview_edit_pending"] = False

    preview = context.chat_data.get("send_preview")
    if isinstance(preview, dict):
        fixed = list(preview.get("fixed") or [])
        fixed.append({"from": old_raw, "to": new_email})
        preview["fixed"] = fixed
        context.chat_data["send_preview"] = preview

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Да", callback_data=f"{_REFRESH_PREFIX}yes"),
                InlineKeyboardButton("Нет", callback_data=f"{_REFRESH_PREFIX}no"),
            ]
        ]
    )
    await _safe_reply_text(update.message, 
        f"✅ Правка сохранена:\n{old_raw} → {new_email}\nОбновить предпросмотр?",
        reply_markup=keyboard,
    )


async def _regenerate_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    if not query or not query.message:
        return False
    chat = query.message.chat
    chat_id = chat.id if chat else 0
    if not chat_id:
        return False

    base_emails = _get_source_emails(context)
    if not base_emails:
        await _safe_reply_text(query.message, "⚠️ Нет исходного списка адресов для обновления.")
        return False

    state = _get_state(context)
    group_code = context.chat_data.get("current_template_code")
    if not group_code and state and getattr(state, "group", None):
        group_code = state.group
    group_code = group_code or ""
    if not group_code:
        await _safe_reply_text(query.message, "⚠️ Сначала выберите направление рассылки.")
        return False

    label = context.chat_data.get("current_template_label") or ""
    if not label and state and getattr(state, "template_label", None):
        label = state.template_label or ""
    if not label and group_code:
        label = get_template_label(group_code)
    template_path = context.chat_data.get("current_template_path") or ""
    if not template_path and state and getattr(state, "template", None):
        template_path = state.template or ""
    if not template_path:
        await _safe_reply_text(query.message, "⚠️ Шаблон письма недоступен. Выберите его заново.")
        return False

    updated_source = apply_saved_edits(list(base_emails), chat_id)
    context.chat_data["preview_source_emails"] = list(updated_source)

    ready, blocked_foreign, blocked_invalid, skipped_recent, _ = (
        messaging.prepare_mass_mailing(
            updated_source,
            group_code,
            chat_id=chat_id,
            ignore_cooldown=bool(context.user_data.get("ignore_cooldown")),
        )
    )

    state_source_map = {}
    if state and getattr(state, "source_map", None):
        try:
            state_source_map = {
                str(k): list(v) if isinstance(v, (list, tuple)) else [v]
                for k, v in state.source_map.items()
                if k
            }
        except Exception:
            state_source_map = {}

    preview_meta = context.chat_data.get("send_preview")
    if isinstance(preview_meta, dict):
        for item in preview_meta.get("fixed", []) or []:
            if not isinstance(item, dict):
                continue
            new_email = item.get("to")
            norm_new = normalize_email(str(new_email or "")) if new_email else ""
            if not norm_new:
                continue
            sources = state_source_map.setdefault(norm_new, [])
            if "manual_edit" not in sources:
                sources.append("manual_edit")

    hits = build_hits_with_sources(ready, state_source_map)
    unique_hits, dup_map = dedupe_across_sources(hits)
    ready = [item.get("email", "") for item in unique_hits if item.get("email")]

    active_norms: set[str] = set()
    for email in ready:
        norm = normalize_email(email) or email.strip().lower()
        if norm:
            active_norms.add(norm)
    active_norms.update(dup_map.keys())
    filtered_sources = {k: state_source_map.get(k, []) for k in active_norms if k}
    context.chat_data["preview_source_map"] = filtered_sources
    context.chat_data["preview_duplicates_global"] = dup_map

    if state:
        state.to_send = ready
        state.group = group_code
        state.template = template_path
        state.template_label = label or state.template_label
        state.source_map = state_source_map

    mass_state.save_chat_state(
        chat_id,
        {
            "group": group_code,
            "template": template_path,
            "template_label": label,
            "pending": ready,
            "blocked_foreign": blocked_foreign,
            "blocked_invalid": blocked_invalid,
            "skipped_recent": skipped_recent,
            "batch_id": context.chat_data.get("batch_id"),
            "source_map": filtered_sources,
        },
    )

    preview = context.chat_data.get("send_preview")
    if isinstance(preview, dict):
        preview["final"] = list(dict.fromkeys(ready))
        preview["global_duplicates"] = dup_map
        context.chat_data["send_preview"] = preview

    await send_preview_report(
        update,
        context,
        group_code,
        label or group_code,
        ready,
        blocked_foreign,
        blocked_invalid,
        skipped_recent,
    )
    return True


async def handle_refresh_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process the user choice after saving an edit."""

    query = update.callback_query
    await safe_answer(query, cache_time=0)
    data = query.data or ""
    _, _, choice = data.partition(":")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:  # pragma: no cover - best effort
        pass
    if choice == "yes":
        success = await _regenerate_preview(update, context)
        if not success:
            await _safe_reply_text(query.message, "❌ Не удалось обновить предпросмотр.")
    else:
        await _safe_reply_text(query.message, "Предпросмотр оставлен без изменений.")
