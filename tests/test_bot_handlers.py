import sys
from pathlib import Path
import types
import asyncio
import logging

import pytest
from telegram import InlineKeyboardMarkup

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
    def __init__(self, text: str | None = None, document=None, chat_id: int = 123):
        self.text = text
        self.document = document
        self.replies: list[str] = []
        self.reply_markups: list = []
        self.chat = types.SimpleNamespace(id=chat_id)

    async def reply_text(self, text, reply_markup=None):
        self.replies.append(text)
        self.reply_markups.append(reply_markup)
        return self


class DummyUpdate:
    def __init__(
        self,
        text: str | None = None,
        document=None,
        chat_id: int = 123,
        callback_data: str | None = None,
    ):
        self.message = DummyMessage(text=text, document=document, chat_id=chat_id)
        self.effective_chat = types.SimpleNamespace(id=chat_id)
        if callback_data is not None:
            self.callback_query = types.SimpleNamespace(
                data=callback_data, message=DummyMessage(chat_id=chat_id)
            )


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


def test_select_group_sets_html_template():
    update = DummyUpdate(callback_data="group_спорт")
    ctx = DummyContext()
    ctx.chat_data[SESSION_KEY] = SessionState(to_send=["a@example.com"])

    run(bh.select_group(update, ctx))

    state = ctx.chat_data[SESSION_KEY]
    assert state.template.endswith((".htm", ".html"))


def test_send_manual_email_uses_html_template(monkeypatch):
    update = DummyUpdate(callback_data="manual_group_туризм")
    ctx = DummyContext()
    ctx.user_data["manual_emails"] = ["user@example.com"]

    sent_paths = []

    def fake_send(client, imap, folder, addr, path, *a, **kw):
        sent_paths.append(path)

    class DummyImap:
        def login(self, *a, **k):
            return "OK", None

        def list(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", None

        def logout(self):
            return

    class DummyClient:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(bh, "SmtpClient", lambda *a, **k: DummyClient())
    monkeypatch.setattr(bh, "imaplib", types.SimpleNamespace(IMAP4_SSL=lambda *a, **k: DummyImap()))
    monkeypatch.setattr(bh, "send_email_with_sessions", fake_send)
    monkeypatch.setattr(bh, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(bh, "get_sent_today", lambda: set())
    monkeypatch.setattr(bh, "was_emailed_recently", lambda *a, **k: False)
    monkeypatch.setattr(bh, "log_sent_email", lambda *a, **k: None)
    monkeypatch.setattr(bh, "clear_recent_sent_cache", lambda: None)
    monkeypatch.setattr(bh, "disable_force_send", lambda chat_id: None)

    async def dummy_sleep(_):
        return

    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)

    run(bh.send_manual_email(update, ctx))

    assert sent_paths and sent_paths[0].endswith((".htm", ".html"))


@pytest.mark.asyncio
async def test_manual_input_parsing_accepts_gmail(caplog):
    update = DummyUpdate(text="taravskayavm@gmail.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True
    with caplog.at_level(logging.INFO):
        await handle_text(update, ctx)
    assert ctx.user_data["manual_emails"] == ["taravskayavm@gmail.com"]
    assert ctx.user_data["awaiting_manual_email"] is False
    assert isinstance(update.message.reply_markups[0], InlineKeyboardMarkup)
    assert any("Manual input parsing" in r.getMessage() for r in caplog.records)