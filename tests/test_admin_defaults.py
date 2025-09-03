import asyncio
import types
import pytest

import emailbot.bot_handlers as bh
import emailbot.settings_store as store
from emailbot.settings_store import get, set


class DummyMessage:
    def __init__(self, text: str | None = None, chat_id: int = 123):
        self.text = text
        self.chat = types.SimpleNamespace(id=chat_id)
        self.replies: list[str] = []
        self.reply_markups: list | None = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append(text)
        self.reply_markups.append(reply_markup)
        return self


class DummyQuery:
    def __init__(self, data: str, chat_id: int):
        self.data = data
        self.message = DummyMessage(chat_id=chat_id)
        self.from_user = types.SimpleNamespace(id=chat_id)

    async def answer(self, *a, **k):
        return

    async def edit_message_text(self, text, reply_markup=None):
        await self.message.reply_text(text, reply_markup=reply_markup)


class DummyUpdate:
    def __init__(self, text: str | None = None, chat_id: int = 123, callback_data: str | None = None):
        self.message = DummyMessage(text=text, chat_id=chat_id)
        self.effective_user = types.SimpleNamespace(id=chat_id)
        if callback_data is not None:
            self.callback_query = DummyQuery(callback_data, chat_id)


class DummyContext:
    def __init__(self):
        self.chat_data: dict = {}
        self.user_data: dict = {}


def run(coro):
    return asyncio.run(coro)


def test_defaults_and_reset(monkeypatch, tmp_path):
    path = tmp_path / "settings.json"
    monkeypatch.setattr(store, "SETTINGS_PATH", path)
    store._cache = None
    store._mtime = 0.0

    assert get("STRICT_OBFUSCATION") is True
    assert get("FOOTNOTE_RADIUS_PAGES") == 1
    assert get("PDF_LAYOUT_AWARE") is False
    assert get("ENABLE_OCR") is False

    set("STRICT_OBFUSCATION", False)
    set("FOOTNOTE_RADIUS_PAGES", 2)
    set("PDF_LAYOUT_AWARE", True)
    set("ENABLE_OCR", True)

    assert get("STRICT_OBFUSCATION") is False

    monkeypatch.setattr(bh, "ADMIN_IDS", {123})
    update = DummyUpdate(callback_data="feat:reset:defaults", chat_id=123)
    ctx = DummyContext()
    run(bh.features_callback(update, ctx))

    assert get("STRICT_OBFUSCATION") is True
    assert get("FOOTNOTE_RADIUS_PAGES") == 1
    assert get("PDF_LAYOUT_AWARE") is False
    assert get("ENABLE_OCR") is False

    monkeypatch.setattr(bh, "ADMIN_IDS", {999})
    update2 = DummyUpdate(text="/features", chat_id=1)
    ctx2 = DummyContext()
    run(bh.features(update2, ctx2))
    assert update2.message.replies == ["Команда доступна только администратору."]
