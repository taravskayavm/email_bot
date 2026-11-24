import asyncio
import logging
import smtplib
import types
from unittest.mock import AsyncMock

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
    async def download_to_drive(self, path, **kwargs):
        return


class DummyDocument:
    file_name = "test.txt"

    async def get_file(self, **kwargs):
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
        self.bot_data: dict = {}


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
    assert "🚀 Игнорировать дневной лимит 300 писем" in labels
    assert "🚀 Игнорировать лимит" not in labels
    assert "🚫 Добавить в блок-лист" in labels
    assert "📄 Показать блок-лист" in labels
    assert "🌐 Добавить исключённый домен" in labels
    assert "📵 Исключённые домены" in labels
    assert "🔄 Синхронизировать с сервером" not in labels
    assert "🔁 Синхронизировать бонсы" not in labels
    rows = [
        [getattr(button, "text", button) for button in row]
        for row in markup.keyboard
    ]
    assert rows[:5] == [
        ["📤 Массовая", "✉️ Ручная"],
        ["🛑 Стоп", "🧹 Очистить список"],
        ["🧭 Сменить группу", "📈 Отчёты"],
        ["🚀 Игнорировать дневной лимит 300 писем"],
        ["⚠️ Игнорировать правило 180 дней"],
    ]


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
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(bh.messaging, "prepare_mass_mailing",
                        lambda emails, **kwargs: (list(emails), [], [], [], {}))
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


def test_block_email_prompt_is_not_consumed_by_text_router():
    update = DummyUpdate(text="🚫 Добавить в блок-лист")
    ctx = DummyContext()

    with pytest.raises(ApplicationHandlerStop):
        run(bh.add_block_prompt(update, ctx))

    assert ctx.user_data["awaiting_block_email"] is True
    assert len(update.message.replies) == 1


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


def test_blocked_domain_prompt_waits_for_the_next_message(monkeypatch):
    ctx = DummyContext()
    prompt_update = DummyUpdate(text="🌐 Добавить исключённый домен")

    with pytest.raises(ApplicationHandlerStop):
        run(bh.add_blocked_domain_prompt(prompt_update, ctx))

    assert ctx.user_data["awaiting_block_domain"] is True
    assert len(prompt_update.message.replies) == 1

    added: list[str] = []
    monkeypatch.setattr(
        bh.blocked_domains,
        "add_blocked_domains",
        lambda domains: added.extend(domains) or len(domains),
    )
    monkeypatch.setattr(
        bh.blocked_domains,
        "load_blocked_domains",
        lambda: {"yahoo.com"},
    )

    domain_update = DummyUpdate(text="yahoo.com")
    with pytest.raises(ApplicationHandlerStop):
        run(bh.route_text_message(domain_update, ctx))

    assert added == ["yahoo.com"]
    assert ctx.user_data["awaiting_block_domain"] is False
    assert domain_update.message.replies == [
        "Добавлено исключённых доменов: 1\nВсего в списке: 1"
    ]


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
        ("123@site.com", "looks-like-footnote"),
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


def test_manual_router_accepts_email_mislabeled_as_url(monkeypatch):
    text = "natalia.kushnarenko1@gmail.com usсhakova.om@yandex.ru"
    update = DummyUpdate(text=text)
    update.message.entities = [
        types.SimpleNamespace(type="url", offset=31, length=23)
    ]
    ctx = DummyContext()
    ctx.user_data["awaiting_manual_email"] = True

    monkeypatch.setattr(
        bh, "should_skip_by_cooldown", lambda email, days=None: (False, "")
    )

    with pytest.raises(ApplicationHandlerStop):
        run(bh.route_text_message(update, ctx))

    assert set(ctx.chat_data["manual_emails"]) == {
        "natalia.kushnarenko1@gmail.com",
        "uschakova.om@yandex.ru",
    }
    assert bh.MANUAL_URL_REJECT_MESSAGE not in update.message.replies


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
async def test_select_group_prepares_queue_and_start_button(monkeypatch, tmp_path):
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

    markup = update.callback_query.message.reply_markups[-1]
    assert isinstance(markup, InlineKeyboardMarkup)
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard
        for button in row
    ]
    batch_id = ctx.chat_data["bulk_handler"]["batch_id"]
    assert callbacks == [f"bulk_start:{batch_id}"]
    batch = ctx.bot_data["bulk_batches"][batch_id]
    assert batch["emails"] == ["a@example.com"]
    assert batch["template"] == str(tpl_path)
    assert batch["group"] == "tourism"
    assert "Готово к отправке" in update.callback_query.message.replies[-1]



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












def test_preview_separates_foreign(monkeypatch):
    ctx = DummyContext()
    allowed_all = {
        "user@ncfu.ru",
        "user@gmail.com",
        "user@gmail.com.br",
    }
    filtered = ["user@ncfu.ru", "user@gmail.com"]
    foreign = ["user@gmail.com.br"]
    monkeypatch.setattr(
        bh.messaging,
        "prepare_mass_mailing",
        lambda emails, **_kwargs: (list(emails), [], [], [], {}),
    )
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
        bh.messaging,
        "prepare_mass_mailing",
        lambda _emails, **_kwargs: (
            ["ready@example.ru"],
            [],
            ["blocked@example.ru"],
            ["recent@example.ru"],
            {},
        ),
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


def test_parse_report_uses_the_same_final_preflight_count(monkeypatch):
    ctx = DummyContext()
    filtered = [f"user{i}@example.ru" for i in range(147)]
    blocked = filtered[:11]
    recent = filtered[11:86]
    ready = filtered[86:]

    monkeypatch.setattr(
        bh.messaging,
        "prepare_mass_mailing",
        lambda _emails, **_kwargs: (ready, [], blocked, recent, {}),
    )
    monkeypatch.setattr(bh, "check_email", lambda _email, **_kwargs: (True, ""))

    report = run(
        bh._compose_report_and_save(
            ctx,
            set(filtered),
            filtered,
            [],
            [],
        )
    )

    state = ctx.chat_data[SESSION_KEY]
    assert len(state.preview_allowed_all) == 61
    assert state.blocked_after_parse == 11
    assert state.cooldown_preview_total == 75
    assert "61" in report


def test_cooldown_preview_samples_from_all_hits(monkeypatch):
    ctx = DummyContext()
    filtered = [f"recent{i}@example.ru" for i in range(5)]

    monkeypatch.setattr(
        bh.messaging,
        "prepare_mass_mailing",
        lambda _emails, **_kwargs: ([], [], [], list(filtered), {}),
    )
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


@pytest.fixture(autouse=True)
def isolate_handler_state(monkeypatch, tmp_path):
    monkeypatch.setattr(bh.mass_state, "STATE_PATH", tmp_path / "mass_state.json")
    monkeypatch.setattr(bh.mass_state, "_state_cache", None)


def test_enable_manual_text_corrections():
    ctx = DummyContext()
    ctx.user_data["manual_emails"] = ["old@example.com"]
    update = DummyUpdate(callback_data="enable_text_corrections")
    run(bh.enable_text_corrections(update, ctx))
    assert ctx.user_data["text_corrections"] is True
    assert ctx.user_data["state"] == MANUAL_WAIT_INPUT
    assert "старый -> новый" in update.callback_query.message.replies[-1]


@pytest.mark.parametrize("new,expected", [
    ("new@example.com", "new@example.com"),
    ("still-bad", "old@example.com"),
])
def test_manual_text_replacement_validates_address(new, expected):
    ctx = DummyContext()
    bh._update_manual_storage(ctx, ["old@example.com"])
    run(bh.enable_text_corrections(DummyUpdate(callback_data="enable_text_corrections"), ctx))
    update = DummyUpdate(text=f"old@example.com -> {new}")
    with pytest.raises(ApplicationHandlerStop):
        run(bh.route_text_message(update, ctx))
    assert ctx.user_data["manual_emails"] == [expected]
    assert ctx.chat_data["manual_emails"] == [expected]
    assert bool(ctx.user_data.get("text_corrections")) == (new == "still-bad")
    assert update.message.replies


@pytest.mark.asyncio
@pytest.mark.parametrize("ignore,allow,enforce", [
    (False, True, True), (True, True, True),
    (True, False, True), (False, True, False),
])
async def test_active_manual_send_template_and_cooldown(monkeypatch, tmp_path, ignore, allow, enforce):
    template = tmp_path / "tourism.html"
    template.write_text("<html>Test</html>", encoding="utf-8")
    monkeypatch.setitem(bh.messaging.TEMPLATE_MAP, "tourism", str(template))
    monkeypatch.setenv("MANUAL_DAYS", "180")
    monkeypatch.setenv("MANUAL_ENFORCE_180", str(int(enforce)))
    monkeypatch.setenv("MANUAL_ALLOW_OVERRIDE", str(int(allow)))
    monkeypatch.setattr(bh, "_store_mass_summary", lambda *a, **k: None)
    monkeypatch.setattr(bh, "disable_force_send", lambda *a: None)
    captured = {}
    def prepare(emails, group, **kwargs):
        captured.update(kwargs)
        return list(emails), [], [], [], {}
    send = AsyncMock(return_value=bh.ManualBatchResult(sent_count=1, remaining=[]))
    monkeypatch.setattr(bh.messaging, "prepare_mass_mailing", prepare)
    monkeypatch.setattr(bh, "_send_batch_with_sessions", send)
    ctx = DummyContext()
    bh._update_manual_storage(ctx, ["user@example.com"])
    ctx.user_data["ignore_180d"] = ignore
    update = DummyUpdate(callback_data="manual_group_tourism")
    await bh.manual_select_group(update, ctx)
    await ctx.chat_data["manual_send_task"]
    assert captured["ignore_cooldown"] == (not enforce or (ignore and allow))
    assert captured["lookback_days"] == (180 if enforce else 0)
    assert send.await_args.args[2:] == (["user@example.com"], str(template), "tourism")
    assert ctx.chat_data["manual_emails"] == []
    assert ctx.user_data.get("manual_emails", []) == []
    assert not ctx.chat_data.get("manual_send_running")


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["blocked", "recent"])
async def test_active_manual_send_excluded_addresses_not_sent(monkeypatch, reason):
    monkeypatch.setenv("MANUAL_ENFORCE_180", "1")
    monkeypatch.setenv("MANUAL_DAYS", "180")
    monkeypatch.setattr(bh, "_store_mass_summary", lambda *a, **k: None)
    def prepare(emails, group, **kwargs):
        return [], [], list(emails) if reason == "blocked" else [], list(emails) if reason == "recent" else [], {}
    send = AsyncMock()
    monkeypatch.setattr(bh.messaging, "prepare_mass_mailing", prepare)
    monkeypatch.setattr(bh, "_send_batch_with_sessions", send)
    ctx = DummyContext()
    bh._update_manual_storage(ctx, ["user@example.com"])
    update = DummyUpdate(callback_data="manual_group_tourism")
    await bh.manual_select_group(update, ctx)
    send.assert_not_awaited()
    text = "\n".join(update.callback_query.message.replies)
    assert ("заблокированных" if reason == "blocked" else "180 дней") in text
    assert "Никого не осталось" in text
    assert "manual_send_task" not in ctx.chat_data


@pytest.mark.parametrize("text", [
    "missing@example.com -> new@example.com",
    "old@example.com -> new@example.com\nother@example.com -> invalid",
])
def test_manual_replacements_do_not_lose_original_list(text):
    ctx = DummyContext()
    original = ["old@example.com", "other@example.com"]
    bh._update_manual_storage(ctx, original)
    run(bh.enable_text_corrections(DummyUpdate(callback_data="enable_text_corrections"), ctx))
    with pytest.raises(ApplicationHandlerStop):
        run(bh.route_text_message(DummyUpdate(text=text), ctx))
    assert ctx.user_data["manual_emails"] == original
    assert ctx.chat_data["manual_emails"] == original
    assert ctx.user_data["text_corrections"] is True
