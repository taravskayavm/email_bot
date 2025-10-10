"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

import io
import re
from typing import Iterable

from aiogram import F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery
from aiogram.utils.markdown import hcode

from emailbot.messaging_utils import is_blocked, is_suppressed
from emailbot.pipelines.ingest import ingest_emails
from emailbot.pipelines.ingest_url import ingest_url_once
from emailbot.settings import resolve_label
from emailbot.utils.file_email_extractor import ExtractError, extract_emails_from_bytes

router = Router()
URL_RE = re.compile(
    r"""(?ix)
    (?<!@)\b(
        (?:https?://|www\.)[^\s<>()]+
        |
        (?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+
        (?:[a-z][a-z0-9-]*[a-z])
        (?:/[^\s<>()]*)?
    )
    (?=$|[\s,;:!?)}\]])
    """
)
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


@router.message(F.text.func(lambda t: bool(t) and URL_RE.search(t)))
async def handle_url_ingest(msg: types.Message) -> None:
    raw = [m.group(1) for m in URL_RE.finditer(msg.text)]
    urls = []
    for u in raw:
        u = u.rstrip('.,;:!?)]}')
        if not u.lower().startswith(('http://','https://')):
            u = 'https://' + u
        urls.append(u)
    ack = await msg.reply("Приняла ссылку, парсю страницу…")
    total_ok: list[str] = []
    total_rejects: dict[str, int] = {}
    errors: list[str] = []
    for url in urls:
        try:
            ok, rejects = await ingest_url_once(url)
        except Exception as exc:  # pragma: no cover - network errors are variable
            errors.append(f"{url} — {type(exc).__name__}")
            continue
        filtered = _filter_stoplists(ok)
        total_ok.extend(filtered)
        for key, val in (rejects or {}).items():
            total_rejects[key] = total_rejects.get(key, 0) + val
    total_ok = list(dict.fromkeys(total_ok))
    text = f"Готово.\nНайдено адресов: {len(total_ok)}"
    text += _format_rejects(total_rejects)
    if total_ok:
        text += "\n\nПримеры:\n" + "\n".join(hcode(x) for x in total_ok[:5])
    if errors:
        text += "\n\nНе удалось загрузить:\n" + "\n".join(f" • {err}" for err in errors)
    try:
        await ack.edit_text(text)
    except TelegramBadRequest:  # message might be gone
        await msg.answer(text)


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
