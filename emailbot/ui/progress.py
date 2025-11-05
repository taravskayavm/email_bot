"""Lightweight progress indicator shown in Telegram chats."""

from __future__ import annotations

from typing import Optional


class ProgressUI:
    """Track and update progress messages in a chat."""

    def __init__(self, bot, chat_id: int):
        self.bot = bot
        self.chat_id = chat_id
        self.msg_id: Optional[int] = None
        self.total = 0
        self.processed = 0

    async def start(self, total: int) -> None:
        self.total = max(0, int(total))
        text = f"Начинаю рассылку… Отправлено 0 из {self.total} (0%)"
        message = await self.bot.send_message(self.chat_id, text)
        self.msg_id = message.message_id

    async def update(self, processed: int) -> None:
        self.processed = max(0, int(processed))
        if self.total <= 0 or self.msg_id is None:
            return
        pct = 0
        if self.total:
            pct = int(min(self.processed, self.total) * 100 / self.total)
        text = f"Отправлено {min(self.processed, self.total)} из {self.total} ({pct}%)"
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.msg_id,
                text=text,
            )
        except Exception:
            # Прогресс — best-effort, игнорируем ошибки Telegram API.
            pass

    async def finish(self) -> None:
        if self.msg_id is None:
            return
        text = f"Готово. Отправлено {self.processed} из {self.total}."
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id,
                message_id=self.msg_id,
                text=text,
            )
        except Exception:
            pass
