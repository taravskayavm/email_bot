"""Helpers for editing progress messages in Telegram chats."""

from __future__ import annotations

import asyncio
import time
from typing import Callable, Optional

from telegram import Message
from telegram.error import BadRequest

from emailbot.cancel_token import is_cancelled


class Heartbeat:
    """Rate-limit progress message updates to avoid Telegram flood limits."""

    def __init__(
        self,
        msg: Message,
        interval_sec: float = 5.0,
        *,
        supplier: Optional[Callable[[], Optional[str]]] = None,
    ):
        self._msg = msg
        self._interval = max(0.0, float(interval_sec))
        self._last_sent = 0.0
        self._last_text: Optional[str] = None
        self._supplier = supplier
        self._task: Optional[asyncio.Task[None]] = None
        self._stopped = False

    async def tick(self, text: Optional[str]) -> bool:
        """Update ``msg`` with ``text`` if allowed by the interval policy."""

        if not text or is_cancelled():
            return False
        now = time.monotonic()
        if text != self._last_text:
            self._last_text = text
        elif now - self._last_sent < self._interval:
            return False
        self._last_sent = now
        try:
            await self._msg.edit_text(text)
            return True
        except BadRequest as exc:
            message = str(getattr(exc, "message", exc)).lower()
            if "message is not modified" in message or "not found" in message:
                return True
        except Exception:
            pass
        return False

    async def force(self, text: Optional[str]) -> bool:
        """Immediately update the message, bypassing the throttle interval."""

        if not text or is_cancelled():
            return False
        self._last_text = text
        self._last_sent = time.monotonic()
        try:
            await self._msg.edit_text(text)
            return True
        except Exception:
            return False

    def start(self) -> None:
        """Start background updates using the configured ``supplier``."""

        if self._supplier is None:
            return
        if self._task and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        self._stopped = False
        self._task = loop.create_task(self._run())

    def stop(self) -> None:
        """Stop background updates."""

        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _run(self) -> None:
        try:
            while not self._stopped and not is_cancelled():
                text: Optional[str]
                try:
                    text = self._supplier() if self._supplier else None
                except Exception:
                    text = None
                if text:
                    await self.tick(text)
                interval = self._interval if self._interval > 0 else 0.5
                await asyncio.sleep(max(0.5, interval))
        except asyncio.CancelledError:
            pass


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
