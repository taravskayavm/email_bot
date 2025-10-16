"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

import io
import re
from typing import Iterable

from aiogram import F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hcode

from emailbot.messaging_utils import is_blocked, is_suppressed
from emailbot.pipelines.ingest import ingest_emails
from emailbot.pipelines.ingest_url import ingest_url
from emailbot.settings import resolve_label
from emailbot.utils.file_email_extractor import ExtractError, extract_emails_from_bytes
from emailbot.ui.messages import format_parse_summary

router = Router()
URL_RE = re.compile(r"""(?ix)\b((?:https?://)?(?:www\.)?[^\s<>()]+?\.[^\s<>()]{2,}[^\s<>()]*)(?=$|[\s,;:!?)}\]])""")
REJECT_LABELS = {
    "no_at_sign": "нет символа @",
    "empty_local_or_domain": "пустая локаль/домен",
    "local_not_ascii": "локальная часть не ASCII",
    "local_edge_dot": "точка в начале/конце локали",
    "local_consecutive_dots": "две точки подряд в локали",
    "local_bad_chars": "недопустимые символы в локали",
    "domain_bad_shape": "некорректный домен",
    "domain_idna_fail": "ошибка IDNA-кодирования домена",
    "domain_too_long": "слишком длинный домен",
    "domain_label_size": "длина лейбла домена неверна",
    "domain_label_dash": "лейбл домена начинается/заканчивается дефисом",
    "missing_dep_openpyxl": "нет зависимости openpyxl для .xlsx",
    "missing_dep_python_docx": "нет зависимости python-docx для .docx",
    "missing_dep_pdfminer": "нет зависимости pdfminer.six для .pdf",
    "unknown": "иная причина",
}


_LAST_URLS: dict[int, str] = {}


def _format_rejects(rejects: dict[str, int], mapping: dict[str, str] | None = None) -> str:
    if not rejects:
        return ""
    mapping = mapping or REJECT_LABELS
    lines = ["\nПричины отбраковки:"]
    for code, count in rejects.items():
        lines.append(f" • {mapping.get(code, code)} — {count}")
    return "\n".join(lines)


def _filter_stoplists(addresses: Iterable[str]) -> list[str]:
    return [email for email in addresses if not (is_blocked(email) or is_suppressed(email))]


@router.message(F.text & F.text.startswith("/ingest"))
async def handle_ingest(msg: types.Message) -> None:
    """Process `/ingest` command with newline separated addresses."""

    lines = [line for line in msg.text.splitlines()[1:] if line.strip()]
    ok, bad, stats = ingest_emails(lines)
    text = (
        f"Всего строк: {stats['total_in']}\n"
        f"Годных адресов: {stats['ok']}\n"
        f"Отброшено: {stats['bad']}"
    )
    rejects = stats.get("rejects") or {}
    text += _format_rejects(rejects)
    if ok:
        text += "\n\nПримеры:\n" + "\n".join(hcode(x) for x in ok[:5])
    if bad:
        text += "\n\nОтброшенные строки:\n" + "\n".join(hcode(x) for x in bad[:5])
    await msg.answer(text)


@router.message(F.text.regexp(URL_RE))
async def handle_url(msg: types.Message) -> None:
    if not msg.text:
        return
    if msg.text.startswith("/ingest"):
        return
    match = URL_RE.search(msg.text)
    if not match:
        return
    url = match.group(1)
    url = url.rstrip(".,;:!?)]}")
    if not url.lower().startswith(("http://", "https://")):
        url = f"https://{url}"
    user_id = msg.from_user.id if msg.from_user else None
    if user_id is not None:
        _LAST_URLS[user_id] = url
    builder = InlineKeyboardBuilder()
    builder.button(text="🔎 Парсить эту страницу", callback_data="parse_url:single")
    builder.button(
        text="🕷️ Сканировать сайт (до 50 стр.)",
        callback_data="parse_url:deep",
    )
    builder.adjust(1)
    await msg.answer(
        f"Нашла ссылку:\n{hcode(url)}\nВыберите режим:",
        reply_markup=builder.as_markup(),
    )


async def _process_url_callback(callback: CallbackQuery, *, deep: bool) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    url = _LAST_URLS.get(user_id)
    if not url:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    status_text = (
        f"🕷️ Сканирую сайт (до 50 стр.):\n{hcode(url)}"
        if deep
        else f"🔎 Парсю одну страницу:\n{hcode(url)}"
    )
    try:
        await callback.message.edit_text(status_text)
    except TelegramBadRequest:
        await callback.message.answer(status_text)
    try:
        ok, stats = await ingest_url(url, deep=deep)
    except Exception as exc:  # pragma: no cover - network errors are variable
        await callback.message.answer(
            f"Не удалось обработать ссылку {hcode(url)}: {exc}"
        )
        await callback.answer()
        return
    filtered = list(dict.fromkeys(_filter_stoplists(ok)))
    summary = format_parse_summary(
        {
            "total_found": stats.get("total_in", 0),
            "to_send": len(filtered),
            "suspicious": 0,
            "cooldown_180d": 0,
            "foreign_domain": 0,
            "pages_skipped": 0,
            "footnote_dupes_removed": 0,
            "blocked": stats.get("blocked", 0),
            "blocked_after_parse": stats.get("blocked", 0),
        },
        examples=filtered[:5],
    )
    if filtered:
        summary += "\nПримеры:\n" + "\n".join(hcode(addr) for addr in filtered[:5])
    pages = stats.get("pages", 0)
    if deep and pages:
        summary += f"\n\n🌐 Просканировано страниц: {pages}"
    await callback.message.answer(summary)
    await callback.answer()


@router.callback_query(F.data == "parse_url:single")
async def parse_single(callback: CallbackQuery) -> None:
    await _process_url_callback(callback, deep=False)


@router.callback_query(F.data == "parse_url:deep")
async def parse_deep(callback: CallbackQuery) -> None:
    await _process_url_callback(callback, deep=True)


@router.message(F.document)
async def handle_document(msg: types.Message) -> None:
    doc = msg.document
    ack = await msg.reply(f"Приняла файл: {doc.file_name}. Обрабатываю…")
    buffer = io.BytesIO()
    await msg.bot.download(doc, destination=buffer)
    data = buffer.getvalue()
    try:
        ok, rejects, warn = extract_emails_from_bytes(data, doc.file_name or "file")
    except ExtractError as exc:
        await ack.edit_text(f"Не удалось обработать файл: {exc}")
        return
    except Exception:  # pragma: no cover - unexpected decoding errors
        await ack.edit_text("Произошла ошибка при разборе файла.")
        return

    ok = list(dict.fromkeys(_filter_stoplists(ok)))
    text = f"Готово.\nНайдено адресов: {len(ok)}"
    text += _format_rejects(rejects)
    if warn:
        text += f"\n\n⚠️ {warn}"
    if ok:
        text += "\n\nПримеры:\n" + "\n".join(hcode(x) for x in ok[:5])
    await ack.edit_text(text)


@router.callback_query(F.data.startswith("set_group:"))
async def set_group(callback: CallbackQuery) -> None:
    """Handle group selection from inline keyboard."""

    label = callback.data.split("set_group:", 1)[1]
    slug = resolve_label(label)
    await callback.message.answer(f"Вы выбрали: {label} ({slug})")
    await callback.answer()
