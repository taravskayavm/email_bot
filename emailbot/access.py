"""Shared, fail-closed Telegram access policy for both entrypoints."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
from pathlib import Path

from filelock import FileLock

logger = logging.getLogger(__name__)


def _ids(value: str) -> set[int]:
    return {int(item) for item in re.split(r"[\s,;]+", value.strip()) if item}


class AccessPolicy:
    def __init__(self, user_ids=(), usernames=(), chat_ids=(), bindings_path=None):
        self.user_ids = set(user_ids)
        self.usernames = {name.lower().lstrip("@") for name in usernames}
        self.chat_ids = set(chat_ids)
        self.bindings_path = Path(bindings_path) if bindings_path else (
            Path(__file__).resolve().parents[1] / "var" / "authorized_users.json"
        )

    @classmethod
    def from_env(cls):
        return cls(
            _ids(os.getenv("ALLOWED_USER_IDS", "")) | _ids(os.getenv("ADMIN_IDS", "")),
            re.findall(r"[A-Za-z0-9_]+", os.getenv("ALLOWED_USERNAMES", "")),
            _ids(os.getenv("ALLOWED_CHAT_IDS", "")),
        )

    def allows(self, user, chat) -> bool:
        if user is None or chat is None or getattr(user, "is_bot", False):
            return False
        private = str(chat.type) == "private"
        if not private and chat.id not in self.chat_ids:
            return False
        if user.id in self.user_ids:
            return True
        if not self.usernames:
            return False
        username = (getattr(user, "username", None) or "").lower()
        path = self.bindings_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with FileLock(str(path) + ".lock", timeout=5):
                bindings = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
                if not isinstance(bindings, dict) or any(
                    not isinstance(key, str) or type(value) is not int or value <= 0
                    for key, value in bindings.items()
                ):
                    raise ValueError("Invalid access bindings")
                if any(bindings.get(name) == user.id for name in self.usernames):
                    return True
                # Bootstrap only in a private chat; a bound username cannot be claimed again.
                if not private or username not in self.usernames or username in bindings:
                    return False
                bindings[username] = user.id
                fd, temporary = tempfile.mkstemp(prefix=".authorized-", dir=path.parent)
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as stream:
                        json.dump(bindings, stream)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, path)
                finally:
                    Path(temporary).unlink(missing_ok=True)
                logger.info("Bound authorized Telegram user ID %s", user.id)
                return True
        except Exception:
            logger.exception("Unable to verify Telegram access bindings")
            return False


def ptb_access_guard(policy: AccessPolicy):
    async def guard(update, context):
        from telegram.ext import ApplicationHandlerStop

        if await asyncio.to_thread(policy.allows, update.effective_user, update.effective_chat):
            return
        try:
            if update.callback_query:
                await update.callback_query.answer("Доступ запрещён.", show_alert=True)
            elif update.effective_chat and update.effective_chat.type == "private" and update.effective_message:
                await update.effective_message.reply_text("Доступ запрещён.")
        finally:
            raise ApplicationHandlerStop
    return guard


class AiogramAccessMiddleware:
    def __init__(self, policy: AccessPolicy):
        self.policy = policy

    async def __call__(self, handler, event, data):
        # Registered on the update observer, before routers and their state changes.
        try:
            payload = event.event
        except (AttributeError, LookupError):
            return None
        user = getattr(payload, "from_user", None)
        message = getattr(payload, "message", None)
        chat = getattr(payload, "chat", None) or getattr(message, "chat", None)
        if await asyncio.to_thread(self.policy.allows, user, chat):
            return await handler(event, data)
        return None
