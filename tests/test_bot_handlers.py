import asyncio
import logging
import smtplib
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


def test_main_menu_has_shared_cooldown_toggle():
    update = DummyUpdate(text="/start")
    ctx = DummyContext()

    run(start(update, ctx))

    markup = update.message.reply_markups[0]
    labels = [
        getattr(button, "text", button)
        for row in markup.keyboard
        for button in row
    ]
    assert "⚠️ Игнорировать правило 180 дней" in labels
    assert "🚫 Добавить в блок-лист" in labels
    assert "📄 Показать блок-лист" in labels
    assert "🌐 Добавить исключённый домен" in labels
    assert "📵 Исключённые домены" in labels
    assert "🔄 Синхронизировать с сервером" not in labels
    assert "🔁 Синхронизировать бонсы" not in labels


def test_main_menu_hides_manual_override_when_disabled(monkeypatch):
    monkeypatch.setenv("MANUAL_ALLOW_OVERRIDE", "0")
    update = DummyUpdate(text="/start")
    ctx = DummyContext()

    run(start(update, ctx))

    markup = update.message.reply_markups[0]
    labels = [
        getattr(button, "text", button)
        for row in markup.keyboard
        for button in row
    ]
    assert "⚠️ Игнорировать правило 180 дней" not in labels


def test_main_menu_cooldown_toggle_updates_shared_state():
    update = DummyUpdate(text="⚠️ Игнорировать правило 180 дней")
    ctx = DummyContext()

    with pytest.raises(ApplicationHandlerStop):
        run(bh.toggle_ignore_180_menu(update, ctx))

    assert ctx.user_data["ignore_180d"] is True
    assert ctx.user_data["ignore_cooldown"] is True
    assert ctx.chat_data[SESSION_KEY].override_cooldown is True

    with pytest.raises(ApplicationHandlerStop):
        run(bh.toggle_ignore_180_menu(update, ctx))

    assert ctx.user_data["ignore_180d"] is False
    assert ctx.user_data["ignore_cooldown"] is False
    assert ctx.chat_data[SESSION_KEY].override_cooldown is False


def test_menu_stop_signals_global_and_chat_cancellation(monkeypatch):
    update = DummyUpdate(text="🛑 Стоп", chat_id=123)
    ctx = DummyContext()
    active_event = asyncio.Event()
    ctx.chat_data["cancel_event"] = active_event
    cancelled_chats: list[int] = []
    monkeypatch.setattr(bh, "request_cancel", cancelled_chats.append)
    monkeypatch.setattr(
        bh,
        "stop_and_status",
        lambda: {"stopped": True, "running": {"manual_mass_send": "running"}},
    )

    run(bh.stop_process(update, ctx))

    assert cancelled_chats == [123]
    assert active_event.is_set()
    assert ctx.chat_data["cancel_event"] is not active_event
    assert update.message.replies[0].startswith("🛑 Останавливаю все процессы")


def test_handle_document_processes_file(monkeypatch, tmp_path):
    update = DummyUpdate(document=DummyDocument())
    ctx = DummyContext()

    monkeypatch.setattr(bh, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(
        bh,
        "extract_from_uploaded_file",
        lambda path, stop_event=None: (
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
    assert "Всего найдено: 3" in report
    assert "📦 К отправке: 2" in report
    assert "🌍 Иностранные домены: 1" in report


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
    assert update.message.replies[0] == "Добавлено в блок-лист: 1"


def test_route_text_message_adds_to_blocklist(monkeypatch):
    update = DummyUpdate(text="First@example.com second@example.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_block_email"] = True
    added: list[str] = []
    monkeypatch.setattr(bh, "add_blocked_email", lambda email: not added.append(email))

    with pytest.raises(ApplicationHandlerStop):
        run(bh.route_text_message(update, ctx))

    assert sorted(added) == ["first@example.com", "second@example.com"]
    assert ctx.user_data["awaiting_block_email"] is False
    assert update.message.replies == ["Добавлено в блок-лист: 2"]


def test_handle_text_adds_blocked_domains(monkeypatch):
    update = DummyUpdate(text="Example.com, qq.com bad_domain")
    ctx = DummyContext()
    ctx.user_data["awaiting_block_domain"] = True
    added: list[str] = []

    monkeypatch.setattr(
        bh.blocked_domains,
        "parse_domains",
        lambda text: (["example.com", "qq.com"], ["bad_domain"]),
    )
    monkeypatch.setattr(
        bh.blocked_domains,
        "add_blocked_domains",
        lambda domains: added.extend(domains) or len(domains),
    )
    monkeypatch.setattr(
        bh.blocked_domains,
        "load_blocked_domains",
        lambda: {"example.com", "qq.com"},
    )

    run(handle_text(update, ctx))

    assert added == ["example.com", "qq.com"]
    assert ctx.user_data["awaiting_block_domain"] is False
    assert "Добавлено исключённых доменов: 2" in update.message.replies[0]
    assert "Не распознано: bad_domain" in update.message.replies[0]


def test_selfcheck_offers_server_reconciliation(monkeypatch):
    update = DummyUpdate(text="🩺 Диагностика")
    ctx = DummyContext()
    monkeypatch.setattr(bh, "run_selfcheck", lambda: [])
    monkeypatch.setattr(bh, "format_selfcheck", lambda checks: "diagnostics")

    run(bh.selfcheck_command(update, ctx))

    markup = update.message.reply_markups[-1]
    buttons = [button for row in markup.inline_keyboard for button in row]
    assert [(button.text, button.callback_data) for button in buttons] == [
        ("🔄 Сверить журнал с сервером", "diag_sync_imap")
    ]


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


def test_handle_text_manual_rejects_url():
    update = DummyUpdate(text="https://example.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True

    run(handle_text(update, ctx))

    assert ctx.user_data["awaiting_manual_email"] is True
    assert update.message.replies == [bh.MANUAL_URL_REJECT_MESSAGE]


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


def test_manual_input_router_rejects_url():
    update = DummyUpdate(text="https://example.com")
    ctx = DummyContext()
    ctx.user_data["state"] = MANUAL_WAIT_INPUT
    ctx.user_data["awaiting_manual_email"] = True

    with pytest.raises(ApplicationHandlerStop):
        run(manual_input_router(update, ctx))

    assert ctx.user_data["state"] == MANUAL_WAIT_INPUT
    assert ctx.user_data["awaiting_manual_email"] is True
    assert update.message.replies == [bh.MANUAL_URL_REJECT_MESSAGE]


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

    def fake_prepare(emails, group, chat_id=None, **kwargs):
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


@pytest.mark.asyncio
async def test_active_manual_send_runs_in_background_and_keeps_remainder(
    monkeypatch, tmp_path
):
    template = tmp_path / "tourism.html"
    template.write_text("<html></html>", encoding="utf-8")
    monkeypatch.setitem(bh.messaging.TEMPLATE_MAP, "tourism", str(template))
    monkeypatch.setenv("MANUAL_ENFORCE_180", "1")
    monkeypatch.setenv("MANUAL_DAYS", "90")
    monkeypatch.setenv("MANUAL_ALLOW_OVERRIDE", "1")
    monkeypatch.setattr(bh, "_store_mass_summary", lambda *a, **k: None)
    monkeypatch.setattr(bh, "disable_force_send", lambda *_: None)

    prepare_args: dict[str, object] = {}

    def fake_prepare(emails, group, **kwargs):
        prepare_args.update(kwargs)
        return list(emails), [], [], [], {}

    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_send(*_args, **_kwargs):
        started.set()
        await release.wait()
        return bh.ManualBatchResult(remaining=["two@example.com"], retryable=True)

    monkeypatch.setattr(bh.messaging, "prepare_mass_mailing", fake_prepare)
    monkeypatch.setattr(bh, "_send_batch_with_sessions", fake_send)

    update = DummyUpdate(callback_data="manual_group_tourism", chat_id=42)
    ctx = DummyContext()
    ctx.chat_data["manual_emails"] = ["one@example.com", "two@example.com"]
    ctx.user_data["manual_emails"] = ["one@example.com", "two@example.com"]

    await bh.manual_select_group(update, ctx)
    task = ctx.chat_data["manual_send_task"]
    await started.wait()

    assert not task.done()
    assert prepare_args["lookback_days"] == 90

    release.set()
    await task

    assert ctx.chat_data["manual_emails"] == ["two@example.com"]
    assert ctx.user_data["manual_emails"] == ["two@example.com"]


@pytest.mark.asyncio
async def test_active_manual_batch_preserves_queue_when_daily_limit_is_full(
    monkeypatch
):
    monkeypatch.setattr(bh, "MAX_EMAILS_PER_DAY", 1)
    monkeypatch.setattr(bh, "get_sent_today", lambda: {"already@example.com"})
    monkeypatch.setattr(bh, "is_force_send", lambda _chat_id: False)
    update = DummyUpdate(callback_data="manual_group_tourism", chat_id=42)
    ctx = DummyContext()

    result = await bh._send_batch_with_sessions(
        update.callback_query,
        ctx,
        ["one@example.com", "two@example.com"],
        "unused.html",
        "tourism",
    )

    assert result.retryable is True
    assert result.remaining == ["one@example.com", "two@example.com"]


@pytest.mark.asyncio
async def test_active_manual_batch_retries_same_recipient_after_disconnect(
    monkeypatch
):
    monkeypatch.setattr(bh, "get_sent_today", lambda: set())
    monkeypatch.setattr(bh, "is_force_send", lambda _chat_id: False)
    monkeypatch.setattr(bh, "should_stop", lambda: False)
    monkeypatch.setattr(bh, "is_cancelled", lambda _chat_id: False)
    monkeypatch.setattr(bh, "log_sent_email", lambda *a, **k: None)
    monkeypatch.setattr(bh, "clear_recent_sent_cache", lambda: None)
    monkeypatch.setattr(bh, "mark_soft_bounce_success", lambda *_: None)
    monkeypatch.setattr(bh, "get_preferred_sent_folder", lambda _imap: "Sent")

    async def no_wait(*_args, **_kwargs):
        return None

    monkeypatch.setattr(bh, "heartbeat", no_wait)
    monkeypatch.setattr(bh.asyncio, "sleep", no_wait)

    class DummyImap:
        def login(self, *a, **k):
            return "OK", []

        def select(self, *a, **k):
            return "OK", []

        def logout(self):
            return "BYE", []

    class DummySmtp:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return None

    monkeypatch.setattr(bh.imaplib, "IMAP4_SSL", lambda *a, **k: DummyImap())
    monkeypatch.setattr(bh, "SmtpClient", DummySmtp)

    attempts = 0

    def fake_send(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise smtplib.SMTPServerDisconnected("connection lost")
        return SendOutcome.SENT, "token", "key", "hash"

    monkeypatch.setattr(bh, "send_email_with_sessions", fake_send)
    update = DummyUpdate(callback_data="manual_group_tourism", chat_id=42)
    ctx = DummyContext()

    result = await bh._send_batch_with_sessions(
        update.callback_query,
        ctx,
        ["one@example.com"],
        "template.html",
        "tourism",
    )

    assert attempts == 2
    assert result.sent_count == 1
    assert result.remaining == []


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


def test_manual_input_keyboard_has_no_cooldown_toggle():
    update = DummyUpdate(text="user@example.com")
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True
    run(handle_text(update, ctx))
    markup = update.message.reply_markups[-1]
    assert isinstance(markup, InlineKeyboardMarkup)
    labels = [btn.text for row in markup.inline_keyboard for btn in row]
    assert not any("Игнорировать 180 дней" in text for text in labels)


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
    ctx.user_data["manual_emails"] = ["x@example.com"]

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
    assert "Правило 180 дней" in text
    assert "❗ Все адреса уже есть" in text


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


def test_parse_report_counts_filters_without_overlap(monkeypatch):
    ctx = DummyContext()
    allowed_all = {
        "ready@example.ru",
        "blocked@example.ru",
        "recent@example.ru",
        ".invalid@example.ru",
        "foreign@example.de",
    }
    filtered = [
        "ready@example.ru",
        "blocked@example.ru",
        "recent@example.ru",
    ]

    monkeypatch.setattr(
        bh,
        "is_blocked",
        lambda email: email == "blocked@example.ru",
    )
    monkeypatch.setattr(
        bh,
        "check_email",
        lambda email, **_kwargs: (email == "recent@example.ru", ""),
    )

    report = run(
        bh._compose_report_and_save(
            ctx,
            allowed_all,
            filtered,
            [],
            ["foreign@example.de"],
            invalid=[".invalid@example.ru"],
        )
    )

    assert "Всего найдено: 5" in report
    assert "🌍 Иностранные домены: 1" in report
    assert "❌ Некорректные адреса: 1" in report
    assert "🚫 В стоп-листе: 1" in report
    assert "⏳ Под кулдауном (180 дней): 1" in report
    assert "📦 К отправке: 1" in report
    assert ctx.chat_data[SESSION_KEY].preview_allowed_all == ["ready@example.ru"]


def test_cooldown_preview_samples_from_all_hits(monkeypatch):
    ctx = DummyContext()
    filtered = [f"recent{i}@example.ru" for i in range(5)]

    monkeypatch.setattr(bh, "is_blocked", lambda _email: False)
    monkeypatch.setattr(
        bh,
        "check_email",
        lambda email, **_kwargs: (
            True,
            f"last=2026-07-{int(email.removeprefix('recent').split('@', 1)[0]) + 10:02d}",
        ),
    )
    monkeypatch.setattr(
        bh,
        "_sample_random",
        lambda items, k: list(items)[-k:],
    )

    run(
        bh._compose_report_and_save(
            ctx,
            set(filtered),
            filtered,
            [],
            [],
        )
    )

    examples = ctx.chat_data[SESSION_KEY].cooldown_preview_examples
    assert [email for email, _date in examples] == filtered[-3:]
