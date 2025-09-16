import asyncio
import imaplib
import logging
import types

import pytest
from telegram import InlineKeyboardMarkup

import emailbot.bot_handlers as bh
from emailbot.bot_handlers import (
    SESSION_KEY,
    SessionState,
    handle_document,
    handle_text,
    start,
)


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

    async def reply_text(self, text, reply_markup=None, **kwargs):
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

            class DummyQuery:
                def __init__(self, data, chat_id):
                    self.data = data
                    self.message = DummyMessage(chat_id=chat_id)

                async def answer(self, *a, **k):
                    return

            self.callback_query = DummyQuery(callback_data, chat_id)


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
        lambda path: (
            {"good@example.com", "123@site.com"},
            {"foreign@example.de"},
            {},
        ),
    )
    monkeypatch.setattr(
        bh,
        "collect_repairs_from_files",
        lambda files: [("bad@example.com", "good@example.com")],
    )
    monkeypatch.setattr(
        bh, "apply_numeric_truncation_removal", lambda allowed: (allowed, [])
    )
    monkeypatch.setattr(bh, "sample_preview", lambda items, k: list(items)[:k])

    run(handle_document(update, ctx))

    state = ctx.chat_data[SESSION_KEY]
    assert state.all_emails == {"good@example.com", "123@site.com"}
    assert state.dropped == []
    assert state.foreign == ["foreign@example.de"]
    report = update.message.replies[2]
    assert "Найдено адресов: 2" in report
    assert "📧 К отправке: 2 адресов" in report
    assert "⚠️ Подозрительные: 0 адресов" in report


def test_request_fix_sets_state(monkeypatch):
    update = DummyUpdate(callback_data="fix:0")
    ctx = DummyContext()
    ctx.chat_data["send_preview"] = {
        "final": [],
        "dropped": [("bad@example.com", "invalid-email")],
        "fixed": [],
    }

    run(bh.request_fix(update, ctx))

    assert ctx.chat_data["fix_pending"] == {
        "index": 0,
        "original": "bad@example.com",
    }
    prompt = update.callback_query.message.replies[-1]
    assert "Введите исправленный адрес" in prompt


def test_handle_text_fix_success(monkeypatch):
    update = DummyUpdate(text="new@example.com")
    ctx = DummyContext()
    state = SessionState(
        to_send=[],
        preview_allowed_all=[],
        dropped=[("old@example.com", "invalid-email")],
        foreign=[],
    )
    ctx.chat_data[SESSION_KEY] = state
    ctx.chat_data["send_preview"] = {
        "final": [],
        "dropped": [("old@example.com", "invalid-email")],
        "fixed": [],
    }
    ctx.chat_data["fix_pending"] = {
        "index": 0,
        "original": "old@example.com",
    }

    monkeypatch.setattr(
        "pipelines.extract_emails.run_pipeline_on_text",
        lambda text: (["new@example.com"], []),
    )

    run(handle_text(update, ctx))

    assert ctx.chat_data.get("fix_pending") is None
    preview = ctx.chat_data["send_preview"]
    assert "new@example.com" in preview["final"]
    assert preview["dropped"] == []
    assert {"from": "old@example.com", "to": "new@example.com"} in preview["fixed"]
    assert state.to_send == ["new@example.com"]
    assert state.dropped == []
    assert "✅ Исправлено" in update.message.replies[-1]


def test_handle_text_fix_invalid(monkeypatch):
    update = DummyUpdate(text="still-bad")
    ctx = DummyContext()
    state = SessionState(
        to_send=[],
        preview_allowed_all=[],
        dropped=[("broken@example", "invalid")],
        foreign=[],
    )
    ctx.chat_data[SESSION_KEY] = state
    ctx.chat_data["send_preview"] = {
        "final": [],
        "dropped": [("broken@example", "invalid")],
        "fixed": [],
    }
    ctx.chat_data["fix_pending"] = {
        "index": 0,
        "original": "broken@example",
    }

    monkeypatch.setattr(
        "pipelines.extract_emails.run_pipeline_on_text",
        lambda text: ([], [(text, "invalid-email")]),
    )

    run(handle_text(update, ctx))

    assert ctx.chat_data.get("fix_pending") is not None
    assert ctx.chat_data["send_preview"]["dropped"] == [
        ("broken@example", "invalid")
    ]
    assert "❌ Всё ещё некорректно" in update.message.replies[-1]


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
    update = DummyUpdate(
        text="User@example.com support@support.com 123@site.com 1test@site.com"
    )
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True

    run(handle_text(update, ctx))

    assert ctx.chat_data["manual_all_emails"] == [
        "123@site.com",
        "1test@site.com",
        "support@support.com",
        "user@example.com",
    ]
    assert ctx.user_data["awaiting_manual_email"] is False
    assert "Адреса получены." in update.message.replies[0]


def test_prompt_manual_email_clears_previous_list():
    update = DummyUpdate(text="/manual")
    ctx = DummyContext()
    ctx.chat_data["manual_all_emails"] = ["old@example.com"]
    ctx.user_data["awaiting_block_email"] = True

    run(bh.prompt_manual_email(update, ctx))

    assert "manual_all_emails" not in ctx.chat_data
    assert ctx.user_data["awaiting_manual_email"] is True
    assert ctx.user_data.get("awaiting_block_email") is False


def test_select_group_sets_html_template(monkeypatch, tmp_path):
    tpl_path = tmp_path / "tourism.html"
    tpl_path.write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(
        bh,
        "get_template",
        lambda code: {
            "code": code,
            "label": code.title(),
            "path": str(tpl_path),
        }
        if code == "tourism"
        else None,
    )

    update = DummyUpdate(callback_data="tpl:tourism")
    ctx = DummyContext()
    ctx.chat_data[SESSION_KEY] = SessionState(to_send=["a@example.com"])

    run(bh.select_group(update, ctx))

    state = ctx.chat_data[SESSION_KEY]
    assert state.template == str(tpl_path)


def test_send_manual_email_uses_html_template(monkeypatch, tmp_path):
    tpl_path = tmp_path / "tourism.html"
    tpl_path.write_text("<html></html>", encoding="utf-8")

    monkeypatch.setattr(
        bh,
        "get_template",
        lambda code: {
            "code": code,
            "label": code.title(),
            "path": str(tpl_path),
        }
        if code == "tourism"
        else None,
    )

    update = DummyUpdate(callback_data="manual_tpl:tourism")
    ctx = DummyContext()
    ctx.chat_data["manual_all_emails"] = ["user@example.com"]

    sent_paths = []

    def fake_send(client, imap, folder, addr, path, *a, **kw):
        sent_paths.append(path)
        return "tok"

    class DummyImap:
        def login(self, *a, **k):
            return "OK", None

        def list(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", None

        def logout(self):
            return

    class DummySMTP:
        def close(self):
            return None

    monkeypatch.setattr(bh, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(
        bh, "imaplib", types.SimpleNamespace(IMAP4_SSL=lambda *a, **k: DummyImap())
    )
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


def test_manual_input_parsing_accepts_gmail(caplog):
    update = DummyUpdate(text="taravskayavm@gmail.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True
    with caplog.at_level(logging.INFO):
        run(handle_text(update, ctx))
    assert ctx.chat_data["manual_all_emails"] == ["taravskayavm@gmail.com"]
    assert ctx.user_data["awaiting_manual_email"] is False
    assert isinstance(update.message.reply_markups[0], InlineKeyboardMarkup)
    assert any("Manual input parsing" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_send_manual_email_no_block_mentions(monkeypatch, tmp_path):
    tpl_path = tmp_path / "tourism.html"
    tpl_path.write_text("<html></html>", encoding="utf-8")
    monkeypatch.setattr(
        bh,
        "get_template",
        lambda code: {
            "code": code,
            "label": code.title(),
            "path": str(tpl_path),
        }
        if code == "tourism"
        else None,
    )

    update = DummyUpdate(callback_data="manual_tpl:tourism")
    ctx = DummyContext()
    ctx.chat_data["manual_all_emails"] = ["x@example.com"]

    monkeypatch.setattr(bh, "get_blocked_emails", lambda: {"x@example.com"})
    monkeypatch.setattr(bh, "get_sent_today", lambda: set())

    class DummyImap:
        def login(self, *a, **k):
            return "OK", None

        def list(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", None

        def logout(self):
            return

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *a, **k: DummyImap())

    monkeypatch.setattr(
        bh.messaging,
        "create_task_with_logging",
        lambda coro, _: asyncio.create_task(coro),
    )

    await bh.send_manual_email(update, ctx)
    await asyncio.sleep(0)

    text = "\n".join(update.callback_query.message.replies)
    assert "блок" not in text
    assert "180" not in text


def test_preview_separates_foreign():
    ctx = DummyContext()
    allowed_all = {
        "user@ncfu.ru",
        "user@gmail.com",
        "user@gmail.com.br",
    }
    filtered = ["user@ncfu.ru", "user@gmail.com"]
    foreign = ["user@gmail.com.br"]
    run(
        bh._compose_report_and_save(
            ctx,
            allowed_all,
            filtered,
            [],
            foreign,
            0,
        )
    )
    state = ctx.chat_data[SESSION_KEY]
    assert "user@gmail.com.br" in state.foreign
    assert "user@gmail.com.br" not in state.preview_allowed_all
