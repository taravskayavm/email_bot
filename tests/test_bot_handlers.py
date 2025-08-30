import sys
from pathlib import Path
import types
import asyncio

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import emailbot.bot_handlers as bh
from emailbot.bot_handlers import start, handle_document, handle_text, SESSION_KEY, SessionState


class DummyFile:
    async def download_to_drive(self, path):
        return


class DummyDocument:
    file_name = "test.txt"

    async def get_file(self):
        return DummyFile()


class DummyMessage:
    def __init__(self, text: str | None = None, document=None):
        self.text = text
        self.document = document
        self.replies: list[str] = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append(text)
        return self


class DummyUpdate:
    def __init__(self, text: str | None = None, document=None, chat_id: int = 123):
        self.message = DummyMessage(text=text, document=document)
        self.effective_chat = types.SimpleNamespace(id=chat_id)


class DummyContext:
    def __init__(self):
        self.chat_data: dict = {}
        self.user_data: dict = {}


def run(coro):
    return asyncio.run(coro)


def test_start_initializes_state():
    update = DummyUpdate(text="/start")
    ctx = DummyContext()
    run(start(update, ctx))
    assert SESSION_KEY in ctx.chat_data
    assert isinstance(ctx.chat_data[SESSION_KEY], SessionState)
    assert update.message.replies[0].startswith("Можно загрузить данные")


def test_handle_document_processes_file(monkeypatch, tmp_path):
    update = DummyUpdate(document=DummyDocument())
    ctx = DummyContext()

    monkeypatch.setattr(bh, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(
        bh,
        "extract_from_uploaded_file",
        lambda path: ({"good@example.com", "123@site.com"}, {"foreign@example.de"}),
    )
    monkeypatch.setattr(bh, "collect_repairs_from_files", lambda files: [("bad@example.com", "good@example.com")])
    monkeypatch.setattr(bh, "apply_numeric_truncation_removal", lambda allowed: (allowed, []))
    monkeypatch.setattr(bh, "sample_preview", lambda items, k: list(items)[:k])

    run(handle_document(update, ctx))

    state = ctx.chat_data[SESSION_KEY]
    assert state.all_emails == {"good@example.com"}
    assert state.suspect_numeric == ["123@site.com"]
    assert state.foreign == ["foreign@example.de"]
    report = update.message.replies[2]
    assert "Найдено адресов (.ru/.com): 2" in report
    assert "Уникальных (после базовой очистки): 1" in report


def test_handle_text_add_block(monkeypatch):
    update = DummyUpdate(text="Test@example.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_block_email"] = True
    added: list[str] = []
    monkeypatch.setattr(bh, "add_blocked_email", lambda e: not added.append(e))

    run(handle_text(update, ctx))

    assert ctx.user_data["awaiting_block_email"] is False
    assert added == ["test@example.com"]
    assert update.message.replies[0] == "Добавлено в исключения: 1"


def test_handle_text_manual_emails():
    update = DummyUpdate(text="User@example.com support@support.com 123@site.com 1test@site.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True

    run(handle_text(update, ctx))

    assert ctx.user_data["manual_emails"] == ["1test@site.com", "user@example.com"]
    assert ctx.user_data["awaiting_manual_email"] is False
    assert "К отправке: 1test@site.com, user@example.com" in update.message.replies[0]
