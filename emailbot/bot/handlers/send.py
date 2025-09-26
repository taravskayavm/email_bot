"""Manual send command handler for the aiogram bot."""

from __future__ import annotations

from typing import Tuple

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.filters import CommandObject
from email_validator import EmailNotValidError, validate_email

from emailbot.aiogram_port.messaging import send_one_email
from emailbot.messaging_utils import prepare_recipients_for_send

router = Router()


def _parse_args(raw: str) -> Tuple[str, str, str]:
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 3:
        raise ValueError("not enough parts")
    to_addr = parts[0]
    subject = parts[1]
    body = "|".join(parts[2:]).strip()
    if not to_addr or not subject or not body:
        raise ValueError("empty component")
    return to_addr, subject, body


@router.message(Command("send"))
async def send(message: Message, command: CommandObject) -> None:
    """Parse arguments and delegate to the messaging pipeline."""

    if not command.args:
        await message.answer("Формат: /send email@domain.tld | Тема | Текст")
        return

    try:
        to_addr, subject, body = _parse_args(command.args)
    except ValueError:
        await message.answer(
            "Не удалось разобрать аргументы. Пример: /send x@y.com | Привет | Текст…"
        )
        return

    try:
        result = validate_email(to_addr, check_deliverability=False)
    except EmailNotValidError as exc:
        await message.answer(f"Некорректный адрес: {exc}")
        return

    normalized = result.normalized
    good, dropped, _ = prepare_recipients_for_send([normalized])
    if dropped or not good:
        await message.answer("Адрес отклонён после проверки. Письмо не отправлено.")
        return
    normalized = good[0]
    await message.bot.send_chat_action(message.chat.id, "typing")
    ok, info = await send_one_email(normalized, subject, body, source="telegram_manual")
    trace_id = info.get("trace_id", "—")
    if ok:
        await message.answer(
            f"✅ Отправлено на {info.get('masked_to', normalized)} (trace_id={trace_id})"
        )
    else:
        reason = info.get("reason") or "неизвестная ошибка"
        await message.answer(
            f"⛔ Не отправлено: {reason} (trace_id={trace_id})"
        )
