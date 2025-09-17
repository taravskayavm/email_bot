# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence, TYPE_CHECKING

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from emailbot import history_service
from emailbot.report_preview import PreviewData, build_preview_workbook

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from emailbot.bot_handlers import SessionState


PREVIEW_DIR = Path("var")
_BACK_CALLBACK = "preview_back"


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
        rows.append(
            {
                "email": email,
                "last_sent_at": _format_dt(last),
                "reason": ", ".join(reason_parts),
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
    valid_rows = _collect_valid(ready, group_code, fixed_map, rule_days)
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


def _compose_caption(data: PreviewData, rule_days: int) -> str:
    lines = [f"✉️ Готово к отправке: {len(data.valid)} адресов."]
    if data.rejected_180d:
        lines.append(f"⏳ Отложено по правилу {rule_days} дн.: {len(data.rejected_180d)}")
    if data.blocked:
        lines.append(f"🚫 В исключениях/блок-листах: {len(data.blocked)}")
    if data.suspicious:
        lines.append(f"⚠️ Требует проверки: {len(data.suspicious)}")
    if data.duplicates:
        lines.append(f"🔁 Возможные дубликаты: {len(data.duplicates)}")
    lines.append("Файл-предпросмотр: подробности внутри.")
    return "\n".join(lines)


def _preview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Отправить", callback_data="start_sending"),
                InlineKeyboardButton("Вернуться / Править", callback_data=_BACK_CALLBACK),
            ]
        ]
    )


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
    build_preview_workbook(data, path)
    caption = _compose_caption(data, rule_days)
    keyboard = _preview_keyboard()
    with path.open("rb") as fh:
        await update.callback_query.message.reply_document(
            document=fh,
            filename=path.name,
            caption=caption,
            reply_markup=keyboard,
        )


async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the "Вернуться / Править" button press."""

    query = update.callback_query
    await query.answer()
    preview = context.chat_data.get("send_preview") or {}
    dropped = []
    if isinstance(preview, dict):
        dropped = preview.get("dropped", []) or []
    lines = [
        "Можно вернуться к редактированию списка.",
        "Используйте кнопки «✏️ Исправить №…» в сообщении с анализом выше.",
    ]
    if dropped:
        preview_lines = [
            "", "⚠️ Текущие подозрительные:",
            *(
                f"{idx + 1}) {addr} — {reason}" for idx, (addr, reason) in enumerate(dropped[:10])
            ),
        ]
        lines.extend(preview_lines)
    await query.message.reply_text("\n".join(lines))
