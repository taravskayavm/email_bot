from __future__ import annotations

from pathlib import Path
from typing import Sequence

from aiogram import Bot
from aiogram.types import Message

from bot.handlers import make_actions_kb
from services.excel_exporter import export_emails_xlsx
from services.report_builder import build_summary
from services.selection import pick_for_sending
from utils.geo_domains import is_foreign_email


async def send_report(
    bot: Bot,
    message: Message,
    emails: Sequence[str],
    batch_id: str,
    stats: dict,
    *,
    output_dir: Path | None = None,
) -> Path:
    """Сформировать Excel и отправить его пользователю."""

    output_dir = output_dir or Path("var")
    output_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = output_dir / f"emails_{batch_id}.xlsx"

    export_emails_xlsx(str(xlsx_path), emails)

    ready = pick_for_sending(emails, batch_id, message.chat.id)
    stats = dict(stats)
    stats.setdefault("found", list(emails))
    stats.setdefault("to_send", len(ready))
    stats["foreign_count"] = sum(
        1 for email in stats.get("found", []) if is_foreign_email(email)
    )
    stats["batch_id"] = batch_id
    stats["chat_id"] = message.chat.id

    summary = build_summary(stats)
    keyboard = make_actions_kb(batch_id, chat_id=message.chat.id)

    await bot.send_document(
        message.chat.id,
        xlsx_path.open("rb"),
        caption=summary,
        reply_markup=keyboard,
    )

    return xlsx_path


async def build_send_list(
    emails: Sequence[str], batch_id: str, chat_id: int | None = None
) -> list[str]:
    """Helper that applies 'foreign toggle' to produce final send list."""

    return pick_for_sending(emails, batch_id, chat_id)


__all__ = ["send_report", "build_send_list"]
