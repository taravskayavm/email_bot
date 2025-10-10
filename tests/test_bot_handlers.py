import asyncio
import logging
import types

import pytest
from telegram import InlineKeyboardMarkup
from telegram.ext import ApplicationHandlerStop

pytest.importorskip("emailbot.bot_handlers")

import emailbot.bot_handlers as bh
from emailbot import config as C
from emailbot.messaging import SendOutcome
from emailbot.bot_handlers import (
    MANUAL_WAIT_INPUT,
    SESSION_KEY,
    SessionState,
    handle_document,
    handle_text,
    manual_input_router,
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
        self.documents: list[dict] = []
        self.chat = types.SimpleNamespace(id=chat_id)

    async def reply_text(self, text, reply_markup=None, **kwargs):
        self.replies.append(text)
        self.reply_markups.append(reply_markup)
        return self

    async def edit_text(self, text, reply_markup=None, **kwargs):
        self.replies.append(text)
        self.reply_markups.append(reply_markup)
        return self

    async def reply_document(
        self, document, caption=None, reply_markup=None, filename=None, **kwargs
    ):
        doc_name = None
        if hasattr(document, "name"):
            doc_name = document.name
        elif filename:
            doc_name = filename
        self.documents.append({"name": doc_name, "caption": caption})
        self.replies.append(caption or "")
        self.reply_markups.append(reply_markup)
        close = getattr(document, "close", None)
        if callable(close):
            close()
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
        self.effective_message = self.message
        if callback_data is not None:

            class DummyQuery:
                def __init__(self, data, chat_id):
                    self.data = data
                    self.message = DummyMessage(chat_id=chat_id)

                async def answer(self, *a, **k):
                    return

                async def edit_message_reply_markup(self, reply_markup=None, **kwargs):
                    self.message.reply_markups.append(reply_markup)
                    return self.message

            self.callback_query = DummyQuery(callback_data, chat_id)
            self.effective_message = self.callback_query.message


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
    report = update.message.replies[-1]
    assert "Найдено адресов: 2" in report
    assert "📦 К отправке: 2" in report
    assert "🟡 Подозрительные: 1" in report


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

    assert ctx.chat_data["manual_all_emails"] == ["user@example.com"]
    assert ctx.chat_data["manual_drop_reasons"] == [
        ("123@site.com", "role-like"),
        ("1test@site.com", "role-like"),
        ("support@support.com", "role-like"),
    ]
    assert ctx.user_data["awaiting_manual_email"] is False
    assert "Адреса получены." in update.message.replies[0]
    drop_reply = next(
        (text for text in update.message.replies if "Исключены адреса" in text),
        "",
    )
    assert "support@support.com — role-like" in drop_reply


def test_manual_input_router_summary(monkeypatch):
    update = DummyUpdate(text="User@example.com other@example.com")
    ctx = DummyContext()
    ctx.user_data["state"] = MANUAL_WAIT_INPUT
    ctx.user_data["awaiting_manual_email"] = True

    monkeypatch.setattr(bh, "should_skip_by_cooldown", lambda email, days=None: (False, ""))

    with pytest.raises(ApplicationHandlerStop):
        run(manual_input_router(update, ctx))

    assert ctx.user_data.get("state") is None
    assert ctx.user_data.get("awaiting_manual_email") is False
    assert update.message.replies
    assert update.message.replies[0].startswith("✅ Ручная отправка — предпросмотр")
    assert any("Адреса получены." in text for text in update.message.replies)
    assert set(ctx.chat_data.get("manual_all_emails", [])) == {
        "other@example.com",
        "user@example.com",
    }


def test_prompt_manual_email_clears_previous_list():
    update = DummyUpdate(text="/manual")
    ctx = DummyContext()
    ctx.chat_data["manual_all_emails"] = ["old@example.com"]
    ctx.user_data["awaiting_block_email"] = True

    run(bh.prompt_manual_email(update, ctx))

    assert "manual_all_emails" not in ctx.chat_data
    assert ctx.user_data["awaiting_manual_email"] is True
    assert ctx.user_data["state"] == MANUAL_WAIT_INPUT
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

    monkeypatch.setattr(
        bh.messaging,
        "prepare_mass_mailing",
        lambda emails, group, chat_id=None: (emails, [], [], [], {}),
    )
    monkeypatch.setattr("emailbot.handlers.preview.PREVIEW_DIR", tmp_path)
    monkeypatch.setattr(
        "emailbot.handlers.preview.history_service.get_last_sent", lambda *a, **k: None
    )

    update = DummyUpdate(callback_data="tpl:tourism")
    ctx = DummyContext()
    ctx.chat_data[SESSION_KEY] = SessionState(to_send=["a@example.com"])

    run(bh.select_group(update, ctx))

    state = ctx.chat_data[SESSION_KEY]
    assert state.template == str(tpl_path)


@pytest.mark.asyncio
async def test_select_group_sends_preview_document(monkeypatch, tmp_path):
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

    def fake_prepare(emails, group, chat_id=None):
        return emails, ["blocked-foreign@example.com"], ["blocked@example.com"], [
            "recent@example.com"
        ], {}

    monkeypatch.setattr(bh.messaging, "prepare_mass_mailing", fake_prepare)
    monkeypatch.setattr("emailbot.handlers.preview.PREVIEW_DIR", tmp_path)
    monkeypatch.setattr(
        "emailbot.handlers.preview.history_service.get_last_sent", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "emailbot.handlers.preview.history_service.get_days_rule_default", lambda: 180
    )

    update = DummyUpdate(callback_data="tpl:tourism", chat_id=42)
    ctx = DummyContext()
    ctx.chat_data[SESSION_KEY] = SessionState(to_send=["a@example.com"])
    ctx.chat_data["send_preview"] = {"final": ["a@example.com"], "dropped": [], "fixed": []}

    await bh.select_group(update, ctx)

    path = tmp_path / "preview_42.xlsx"
    assert path.exists()
    markup = update.callback_query.message.reply_markups[-1]
    assert isinstance(markup, InlineKeyboardMarkup)
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
    ]
    assert callbacks == [
        "bulk:send:start",
        "bulk:send:back",
        "bulk:send:edit",
    ]
    assert "Готово к отправке" in update.callback_query.message.replies[-1]
    doc_entry = update.callback_query.message.documents[-1]
    assert doc_entry["name"] and doc_entry["name"].endswith("preview_42.xlsx")


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
        return SendOutcome.SENT, "tok", "log"

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
        bh,
        "imap_connect_ssl",
        lambda *a, **k: DummyImap(),
        raising=False,
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

    monkeypatch.setattr(bh.messaging, "imap_connect_ssl", lambda *a, **k: DummyImap())

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


@pytest.mark.asyncio
async def test_manual_send_override_sets_flag(monkeypatch, tmp_path):
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
    ctx.chat_data["manual_send_mode"] = "all"

    overrides: list[bool | None] = []

    def fake_send(client, imap, folder, addr, path, *a, **kw):
        overrides.append(kw.get("override_180d"))
        return SendOutcome.SENT, "tok", "log"

    class DummyImap:
        def login(self, *a, **k):
            return "OK", []

        def list(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", []

        def logout(self):
            return None

    class DummySMTP:
        def close(self):
            return None

    monkeypatch.setattr(bh, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(
        bh,
        "imap_connect_ssl",
        lambda *a, **k: DummyImap(),
        raising=False,
    )
    monkeypatch.setattr(bh, "send_email_with_sessions", fake_send)
    monkeypatch.setattr(bh, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(bh, "get_sent_today", lambda: set())
    monkeypatch.setattr(bh.rules, "load_blocklist", lambda: [])
    monkeypatch.setattr(bh, "log_sent_email", lambda *a, **k: None)
    monkeypatch.setattr(bh, "clear_recent_sent_cache", lambda: None)
    monkeypatch.setattr(bh, "disable_force_send", lambda chat_id: None)
    tasks: list[asyncio.Task] = []

    def spawn(coro, _):
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    monkeypatch.setattr(bh.messaging, "create_task_with_logging", spawn)

    async def dummy_sleep(_):
        return None

    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)

    await bh.send_manual_email(update, ctx)
    for task in tasks:
        await task

    assert overrides and overrides[0] is True


def test_manual_override_store_selected_filters_candidates():
    ctx = DummyContext()
    ctx.chat_data["manual_override_candidates"] = [
        {"email": "valid@example.com", "reason": "recent"}
    ]

    bh._manual_override_store_selected(
        ctx, {"valid@example.com", "other@example.com"}
    )

    assert ctx.chat_data["manual_override_selected"] == ["valid@example.com"]

    bh._manual_override_store_selected(ctx, {"valid@example.com"})
    assert ctx.chat_data["manual_override_selected"] == ["valid@example.com"]


@pytest.mark.asyncio
async def test_manual_ignore_selected_flow():
    ctx = DummyContext()
    ctx.chat_data["manual_override_candidates"] = [
        {"email": "one@example.com", "reason": "recent"},
        {"email": "two@example.com", "reason": "recent"},
    ]
    ctx.chat_data["manual_override_selected"] = []
    ctx.chat_data["manual_override_days"] = 200

    initial = DummyUpdate(callback_data="manual_ignore_selected:go")
    await bh.manual_ignore_selected(initial, ctx)
    message = initial.callback_query.message
    assert message.replies, "Expected initial list to be rendered"

    toggle = DummyUpdate(callback_data="manual_ignore_selected:toggle:1")
    toggle.callback_query.message = message
    await bh.manual_ignore_selected(toggle, ctx)
    assert ctx.chat_data["manual_override_selected"] == ["two@example.com"]

    apply = DummyUpdate(callback_data="manual_ignore_selected:apply")
    apply.callback_query.message = message
    await bh.manual_ignore_selected(apply, ctx)
    assert ctx.chat_data["manual_override_selected"] == ["two@example.com"]

    clear = DummyUpdate(callback_data="manual_ignore_selected:clear")
    clear.callback_query.message = message
    await bh.manual_ignore_selected(clear, ctx)
    assert ctx.chat_data["manual_override_selected"] == []

    toggle_first = DummyUpdate(callback_data="manual_ignore_selected:toggle:0")
    toggle_first.callback_query.message = message
    await bh.manual_ignore_selected(toggle_first, ctx)
    assert ctx.chat_data["manual_override_selected"] == ["one@example.com"]

    close = DummyUpdate(callback_data="manual_ignore_selected:close")
    close.callback_query.message = message
    await bh.manual_ignore_selected(close, ctx)

    assert "Игнорирование правила 200 дней" in message.replies[-1]
    assert ctx.chat_data["manual_override_selected"] == ["one@example.com"]


@pytest.mark.asyncio
async def test_manual_send_selective_override(monkeypatch, tmp_path):
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

    monkeypatch.setattr(
        bh,
        "_filter_by_180",
        lambda emails, group, days, chat_id=None: (
            ["allowed@example.com"],
            ["recent@example.com"],
        ),
    )

    overrides: dict[str, bool | None] = {}

    def fake_send(client, imap, folder, addr, path, *a, **kw):
        overrides[addr] = kw.get("override_180d")
        return SendOutcome.SENT, "tok", f"key-{addr}"

    class DummyImap:
        def login(self, *a, **k):
            return "OK", []

        def list(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", []

        def logout(self):
            return None

    class DummySMTP:
        def close(self):
            return None

    monkeypatch.setattr(bh, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(
        bh,
        "imap_connect_ssl",
        lambda *a, **k: DummyImap(),
        raising=False,
    )
    monkeypatch.setattr(bh, "send_email_with_sessions", fake_send)
    monkeypatch.setattr(bh, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(bh, "get_sent_today", lambda: set())
    monkeypatch.setattr(bh.rules, "load_blocklist", lambda: [])
    monkeypatch.setattr(bh, "log_sent_email", lambda *a, **k: None)
    monkeypatch.setattr(bh, "clear_recent_sent_cache", lambda: None)
    monkeypatch.setattr(bh, "disable_force_send", lambda chat_id: None)

    tasks: list[asyncio.Task] = []

    def spawn(coro, _):
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    monkeypatch.setattr(bh.messaging, "create_task_with_logging", spawn)

    async def dummy_sleep(_):
        return None

    monkeypatch.setattr(asyncio, "sleep", dummy_sleep)

    update = DummyUpdate(callback_data="manual_tpl:tourism")
    ctx = DummyContext()
    ctx.chat_data["manual_all_emails"] = [
        "allowed@example.com",
        "recent@example.com",
    ]
    ctx.chat_data["manual_send_mode"] = "allowed"
    ctx.chat_data["manual_override_candidates"] = [
        {"email": "recent@example.com", "reason": "cooldown"},
        {"email": "allowed@example.com", "reason": "cooldown"},
    ]
    ctx.chat_data["manual_override_selected"] = ["recent@example.com"]
    ctx.chat_data["manual_override_days"] = 180
    ctx.chat_data["manual_override_page"] = 0

    await bh.send_manual_email(update, ctx)
    for task in tasks:
        await task

    assert overrides["allowed@example.com"] is False
    assert overrides["recent@example.com"] is True

    assert "manual_override_selected" not in ctx.chat_data
    assert "manual_override_candidates" not in ctx.chat_data
    assert "manual_override_page" not in ctx.chat_data
    assert "manual_override_days" not in ctx.chat_data


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
