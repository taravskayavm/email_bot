import asyncio
import types

import pytest
from telegram import InlineKeyboardMarkup

pytest.importorskip("emailbot.bot_handlers")

import emailbot.bot_handlers as bh


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


def test_features_non_admin(monkeypatch):
    monkeypatch.setattr(bh, "ADMIN_IDS", {999})
    update = DummyUpdate(text="/features", chat_id=1)
    ctx = DummyContext()
    run(bh.features(update, ctx))
    assert update.message.replies == ["Команда доступна только администратору."]


def _init_settings(monkeypatch):
    import emailbot.settings as settings

    monkeypatch.setattr(settings, "STRICT_OBFUSCATION", True)
    monkeypatch.setattr(settings, "FOOTNOTE_RADIUS_PAGES", 1)
    monkeypatch.setattr(settings, "PDF_LAYOUT_AWARE", False)
    monkeypatch.setattr(settings, "ENABLE_OCR", False)
    monkeypatch.setattr(settings, "load", lambda: None)
    monkeypatch.setattr(settings, "save", lambda: None)
    return settings


def test_features_admin_flow(monkeypatch):
    monkeypatch.setattr(bh, "ADMIN_IDS", {123})
    _init_settings(monkeypatch)

    update = DummyUpdate(text="/features", chat_id=123)
    ctx = DummyContext()
    run(bh.features(update, ctx))

    markup = update.message.reply_markups[0]
    assert isinstance(markup, InlineKeyboardMarkup)
    buttons = [b.callback_data for row in markup.inline_keyboard for b in row]
    assert buttons == [
        "feat:strict:toggle",
        "feat:radius:0",
        "feat:radius:1",
        "feat:radius:2",
        "feat:layout:toggle",
        "feat:ocr:toggle",
        "feat:reset:defaults",
    ]

    # Toggle strict
    cb = DummyUpdate(callback_data="feat:strict:toggle", chat_id=123)
    run(bh.features_callback(cb, ctx))
    text = cb.callback_query.message.replies[-1]
    assert "STRICT_OBFUSCATION=off" in text
    assert "⚠️" in text

    # Set radius to 2
    cb = DummyUpdate(callback_data="feat:radius:2", chat_id=123)
    run(bh.features_callback(cb, ctx))
    text = cb.callback_query.message.replies[-1]
    assert "FOOTNOTE_RADIUS_PAGES=2" in text
    assert "📝 Радиус сносок: 2." in text

    # Toggle layout on then off
    cb = DummyUpdate(callback_data="feat:layout:toggle", chat_id=123)
    run(bh.features_callback(cb, ctx))
    assert "📄 Учёт макета PDF включён" in cb.callback_query.message.replies[-1]
    cb = DummyUpdate(callback_data="feat:layout:toggle", chat_id=123)
    run(bh.features_callback(cb, ctx))
    assert "📄 Учёт макета PDF выключен" in cb.callback_query.message.replies[-1]

    # Toggle OCR on then off
    cb = DummyUpdate(callback_data="feat:ocr:toggle", chat_id=123)
    run(bh.features_callback(cb, ctx))
    assert "🔍 OCR включён" in cb.callback_query.message.replies[-1]
    cb = DummyUpdate(callback_data="feat:ocr:toggle", chat_id=123)
    run(bh.features_callback(cb, ctx))
    assert "🔍 OCR выключен" in cb.callback_query.message.replies[-1]
