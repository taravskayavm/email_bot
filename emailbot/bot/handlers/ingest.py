"""Handlers for ingest flow powered by aiogram."""

from __future__ import annotations

from aiogram import F, Router, types
from aiogram.types import CallbackQuery
from aiogram.utils.markdown import hcode

from emailbot.pipelines.ingest import ingest_emails
from emailbot.settings import resolve_label

router = Router()


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
    if rejects:
        text += "\nПричины отбраковки:"
        human = {
            "no_at_sign": "нет символа @",
            "empty_local_or_domain": "пустая локаль/домен",
            "local_not_ascii": "локальная часть не ASCII",
            "local_edge_dot": "точка в начале/конце локали",
            "local_consecutive_dots": "две точки подряд в локали",
            "local_bad_chars": "недопустимые символы в локали",
            "domain_bad_shape": "некорректный домен",
            "domain_idna_fail": "ошибка IDNA-кодирования домена",
            "domain_too_long": "слишком длинный домен",
            "domain_label_size": "слишком длинный/короткий лейбл домена",
            "domain_label_dash": "лейбл домена начинается/заканчивается дефисом",
        }
        for code, count in rejects.items():
            text += f"\n • {human.get(code, code)} — {count}"
    if ok:
        text += "\n\nПримеры:\n" + "\n".join(hcode(x) for x in ok[:5])
    if bad:
        text += "\n\nОтброшенные строки:\n" + "\n".join(hcode(x) for x in bad[:5])
    await msg.answer(text)


@router.callback_query(F.data.startswith("set_group:"))
async def set_group(callback: CallbackQuery) -> None:
    """Handle group selection from inline keyboard."""

    label = callback.data.split("set_group:", 1)[1]
    slug = resolve_label(label)
    await callback.message.answer(f"Вы выбрали: {label} ({slug})")
    await callback.answer()
