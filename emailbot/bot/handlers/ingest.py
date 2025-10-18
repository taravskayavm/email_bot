"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

import io
import re
from typing import Any, Iterable

from aiogram import F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hcode

from emailbot.messaging_utils import is_blocked, is_suppressed
from emailbot.pipelines.ingest import ingest_emails
from emailbot.pipelines.ingest_url import ingest_url
from emailbot.reporting import count_blocked
from emailbot.settings import resolve_label
from emailbot.web_extract import fetch_and_extract
from emailbot.crawl import crawl_emails
from emailbot import settings
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

_LIMITS_ATTR = "_page_limits"
_AWAIT_ATTR = "_await_page_limits"


def _get_limit_store(bot: Any) -> dict[int, int]:
    store = getattr(bot, _LIMITS_ATTR, None)
    if not isinstance(store, dict):
        store = {}
        setattr(bot, _LIMITS_ATTR, store)
    return store


def _get_awaiting_users(bot: Any) -> set[int]:
    waiting = getattr(bot, _AWAIT_ATTR, None)
    if not isinstance(waiting, set):
        waiting = set()
        setattr(bot, _AWAIT_ATTR, waiting)
    return waiting


def _is_waiting_for_limit(message: types.Message) -> bool:
    user = message.from_user
    if user is None:
        return False
    waiting = _get_awaiting_users(message.bot)
    return user.id in waiting


def _normalize_page_limit(raw: Any) -> int:
    try:
        value = int(str(raw).strip())
    except Exception:
        value = 50
    if value < 1:
        value = 1
    if value > 500:
        value = 500
    return value


def _prepare_filtered(addresses: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(_filter_stoplists(addresses)))


def _build_summary(
    filtered: list[str],
    stats: dict[str, int],
    *,
    deep: bool,
    limit_pages: int | None = None,
) -> str:
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
    if deep:
        used_limit = stats.get("pages_limit") or limit_pages
        if used_limit:
            summary += f"\n\n🌐 Просканировано страниц: {pages} (лимит {used_limit})"
        elif pages:
            summary += f"\n\n🌐 Просканировано страниц: {pages}"
    return summary


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


def _extract_url_arg(text: str) -> str:
    parts = (text or "").strip().split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


@router.message(F.text.startswith("/url"))
async def parse_single_cmd(message: types.Message) -> None:
    """Парсинг одной страницы по команде /url <ссылка>."""

    url = _extract_url_arg(message.text or "")
    if not url:
        await message.reply("Формат: /url <ссылка>")
        return

    try:
        await message.reply(f"🔎 Парсю одну страницу:\n{hcode(url)}")
        final_url, emails = await fetch_and_extract(url)
    except Exception as exc:  # pragma: no cover - network errors vary
        await message.reply(f"⚠️ Ошибка парсинга: {exc}")
        return

    emails = set(emails or [])
    blocked_num = count_blocked(emails)
    allowed = [email for email in emails if not is_blocked(email)]
    preview = "\n".join(hcode(addr) for addr in sorted(allowed)[:10]) or "—"

    await message.reply(
        f"✅ Готово\n"
        f"URL: {hcode(final_url)}\n"
        f"Найдено адресов: {len(emails)}\n"
        f"🚫 В стоп-листе: {blocked_num}\n"
        f"👉 К рассылке пойдут: {len(allowed)}\n\n"
        f"Примеры:\n{preview}"
    )


@router.message(F.text.startswith("/crawl"))
async def crawl_cmd(message: types.Message) -> None:
    """Глубокий скан по домену: /crawl <ссылка> [limit]."""

    tokens = (message.text or "").strip().split()
    if len(tokens) < 2:
        await message.reply("Формат: /crawl <ссылка> [limit]")
        return

    url = tokens[1].strip()
    limit_override: int | None = None
    if len(tokens) >= 3:
        try:
            limit_override = max(1, int(tokens[2]))
        except Exception:
            limit_override = None

    limit = limit_override or settings.CRAWL_MAX_PAGES_PER_DOMAIN

    try:
        await message.reply(f"🕷️ Сканирую сайт (лимит {limit} стр.):\n{hcode(url)}")
        final_url, emails = await crawl_emails(url, limit)
    except Exception as exc:  # pragma: no cover - network errors vary
        await message.reply(f"⚠️ Ошибка сканирования: {exc}")
        return

    emails = set(emails or [])
    blocked_num = count_blocked(emails)
    allowed = [email for email in emails if not is_blocked(email)]
    preview = "\n".join(hcode(addr) for addr in sorted(allowed)[:10]) or "—"

    await message.reply(
        f"✅ Готово\n"
        f"Старт: {hcode(final_url)}\n"
        f"Найдено адресов: {len(emails)}\n"
        f"🚫 В стоп-листе: {blocked_num}\n"
        f"👉 К рассылке пойдут: {len(allowed)}\n\n"
        f"Примеры:\n{preview}"
    )


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
    if msg.text.startswith(("/ingest", "/url", "/crawl")):
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
        text="🕷️ Сканировать сайт",
        callback_data="parse_url:deep",
    )
    builder.adjust(1)
    await msg.answer(
        f"Нашла ссылку:\n{hcode(url)}\nВыберите режим:",
        reply_markup=builder.as_markup(),
    )


async def _process_url_callback(
    callback: CallbackQuery,
    *,
    deep: bool,
    limit_pages: int | None = None,
) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    url = _LAST_URLS.get(user_id)
    if not url:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    waiting = _get_awaiting_users(callback.message.bot)
    waiting.discard(user_id)
    status_text = (
        f"🕷️ Сканирую сайт (лимит {limit_pages} стр.):\n{hcode(url)}"
        if deep and limit_pages is not None
        else f"🕷️ Сканирую сайт:\n{hcode(url)}"
        if deep
        else f"🔎 Парсю одну страницу:\n{hcode(url)}"
    )
    try:
        await callback.message.edit_text(status_text)
    except TelegramBadRequest:
        await callback.message.answer(status_text)
    if deep and limit_pages is not None:
        _get_limit_store(callback.message.bot)[user_id] = limit_pages
    try:
        ok, stats = await ingest_url(url, deep=deep, limit_pages=limit_pages)
    except Exception as exc:  # pragma: no cover - network errors are variable
        await callback.message.answer(
            f"Не удалось обработать ссылку {hcode(url)}: {exc}"
        )
        await callback.answer()
        return
    filtered = _prepare_filtered(ok)
    summary = _build_summary(filtered, stats, deep=deep, limit_pages=limit_pages)
    await callback.message.answer(summary)
    await callback.answer()


@router.callback_query(F.data == "parse_url:single")
async def parse_single(callback: CallbackQuery) -> None:
    await _process_url_callback(callback, deep=False)


@router.callback_query(F.data == "parse_url:deep")
async def parse_deep(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    url = _LAST_URLS.get(user_id)
    if not url:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    keyboard = InlineKeyboardBuilder()
    for limit in (10, 25, 50, 100):
        keyboard.button(text=f"{limit} стр.", callback_data=f"parse_limit:{limit}")
    keyboard.button(text="Другое…", callback_data="parse_limit:custom")
    keyboard.adjust(2)
    waiting = _get_awaiting_users(callback.message.bot)
    waiting.discard(user_id)
    text = (
        f"🕷️ Сканирование сайта:\n{hcode(url)}\n\nВыберите лимит страниц:"
    )
    try:
        await callback.message.edit_text(text, reply_markup=keyboard.as_markup())
    except TelegramBadRequest:
        await callback.message.answer(text, reply_markup=keyboard.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("parse_limit:"))
async def parse_limit(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id if callback.from_user else None
    if user_id is None:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    url = _LAST_URLS.get(user_id)
    if not url:
        await callback.answer("Не удалось определить ссылку", show_alert=True)
        return
    choice = callback.data.split("parse_limit:", 1)[1]
    waiting = _get_awaiting_users(callback.message.bot)
    if choice == "custom":
        waiting.add(user_id)
        prompt = "Введите лимит страниц числом (1–500):"
        try:
            await callback.message.edit_text(prompt)
        except TelegramBadRequest:
            await callback.message.answer(prompt)
        await callback.answer()
        return
    waiting.discard(user_id)
    limit = _normalize_page_limit(choice)
    await _process_url_callback(callback, deep=True, limit_pages=limit)


@router.message(F.text, F.func(_is_waiting_for_limit))
async def handle_limit_input(msg: types.Message) -> None:
    user_id = msg.from_user.id if msg.from_user else None
    if user_id is None:
        return
    waiting = _get_awaiting_users(msg.bot)
    if user_id not in waiting:
        return
    text = (msg.text or "").strip()
    if not text:
        await msg.answer("Введите лимит страниц числом (1–500).")
        return
    waiting.discard(user_id)
    url = _LAST_URLS.get(user_id)
    if not url:
        await msg.answer("Не удалось определить ссылку для сканирования.")
        return
    limit = _normalize_page_limit(text)
    _get_limit_store(msg.bot)[user_id] = limit
    status_text = f"🕷️ Сканирую сайт (лимит {limit} стр.):\n{hcode(url)}"
    await msg.answer(status_text)
    try:
        ok, stats = await ingest_url(url, deep=True, limit_pages=limit)
    except Exception as exc:  # pragma: no cover - network errors are variable
        await msg.answer(f"Не удалось обработать ссылку {hcode(url)}: {exc}")
        return
    filtered = _prepare_filtered(ok)
    summary = _build_summary(filtered, stats, deep=True, limit_pages=limit)
    await msg.answer(summary)


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
