"""Regression coverage for the project review of 2026-11-06 (offline)."""
import asyncio
import csv
import io
import smtplib
import subprocess
import sys
import threading
import zipfile
from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, Mock

import pytest

from emailbot.access import AccessPolicy, AiogramAccessMiddleware, ptb_access_guard
from emailbot.async_transport import MailWorker


def test_deobfuscation_terminates_and_keeps_hyphen():
    code = "from utils.email_deobfuscate import deobfuscate_text; print(deobfuscate_text('anna-petrova@yandex.ru a\\tb\\tc@mail.ru'))"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=8)
    assert result.returncode == 0, result.stderr
    assert "anna-petrova@yandex.ru" in result.stdout
    assert "abc@mail.ru" in result.stdout


def test_username_binding_survives_rename_and_rejects_reassignment(tmp_path):
    path = tmp_path / "access.json"
    policy = AccessPolicy(usernames=["puzo_ezha"], bindings_path=path)
    user = NS(id=123, username="puzo_ezha", is_bot=False)
    private = NS(id=123, type="private")
    assert not policy.allows(user, NS(id=-1, type="group"))
    assert not path.exists()
    assert policy.allows(user, private)
    reloaded = AccessPolicy(usernames=["puzo_ezha"], bindings_path=path)
    assert reloaded.allows(NS(id=123, username="renamed"), private)
    assert not reloaded.allows(NS(id=456, username="puzo_ezha"), private)
    path.write_text("corrupt")
    assert not reloaded.allows(user, private)


def test_access_requires_user_and_explicit_group_permission(tmp_path):
    user = NS(id=1)
    group = NS(id=-123, type="supergroup")
    assert not AccessPolicy().allows(user, NS(id=1, type="private"))
    assert not AccessPolicy(user_ids=[1]).allows(user, group)
    assert AccessPolicy(user_ids=[1], chat_ids=[-123]).allows(user, group)
    assert not AccessPolicy(user_ids=[1], chat_ids=[-123]).allows(NS(id=2), group)


@pytest.mark.asyncio
async def test_both_entrypoint_guards_reject_callback_before_handler():
    from telegram.ext import ApplicationHandlerStop

    policy = AccessPolicy(user_ids=[1])
    user, chat = NS(id=2), NS(id=2, type="private")
    callback = NS(from_user=user, message=NS(chat=chat), answer=AsyncMock())
    guard = ptb_access_guard(policy)
    with pytest.raises(ApplicationHandlerStop):
        await guard(NS(effective_user=user, effective_chat=chat, callback_query=callback), None)
    callback.answer.assert_awaited_once()
    handler = AsyncMock()
    await AiogramAccessMiddleware(policy)(handler, NS(event=callback), {})
    handler.assert_not_awaited()
    callback.from_user = NS(id=1)
    await AiogramAccessMiddleware(policy)(handler, NS(event=callback), {})
    handler.assert_awaited_once()


@pytest.mark.parametrize("kind", ["simple", "robust", "aiogram"])
@pytest.mark.parametrize("close_fails", [False, True])
def test_starttls_failure_never_sends_password(monkeypatch, kind, close_fails):
    from emailbot.smtp_client import SmtpClient, RobustSMTP
    from emailbot.aiogram_port.smtp_sender import SmtpSender

    server = Mock()
    server.starttls.side_effect = smtplib.SMTPNotSupportedError("no TLS")
    if close_fails:
        server.close.side_effect = OSError("socket close failed")
    monkeypatch.setattr(smtplib, "SMTP", Mock(return_value=server))
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_SSL", "0")
    monkeypatch.setenv("EMAIL_ADDRESS", "test@example.test")
    monkeypatch.setenv("EMAIL_PASSWORD", "dummy")
    with pytest.raises(smtplib.SMTPNotSupportedError):
        if kind == "simple":
            SmtpClient("local", 587, "user", "dummy", use_ssl=False).__enter__()
        elif kind == "robust":
            RobustSMTP().connect()
        else:
            SmtpSender()._connect()
    server.login.assert_not_called()
    server.close.assert_called_once()


def test_unsubscribe_and_parallel_append_preserve_both_rows(tmp_path, monkeypatch):
    from emailbot import messaging_utils as module
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "sent.csv"
    now = datetime.now(timezone.utc)
    module.upsert_sent_log(path, "alice@example.com", now, "test")
    writing, release = threading.Event(), threading.Event()
    original = module._atomic_write

    def paused_write(target, rows, headers):
        if any(row.get("unsubscribed") == "1" for row in rows):
            writing.set()
            assert release.wait(5)
        return original(target, rows, headers)

    monkeypatch.setattr(module, "_atomic_write", paused_write)
    with ThreadPoolExecutor(2) as pool:
        update = pool.submit(module.mark_unsubscribed_in_log, path, "alice@example.com")
        assert writing.wait(5)
        append = pool.submit(module.upsert_sent_log, path, "bob@example.com", now, "test")
        assert not append.done()
        release.set()
        assert update.result(timeout=5)
        append.result(timeout=5)
    with path.open(encoding="utf-8") as stream:
        rows = {row["email"]: row for row in csv.DictReader(stream)}
    assert set(rows) == {"alice@example.com", "bob@example.com"}
    assert rows["alice@example.com"]["unsubscribed"] == "1"


@pytest.mark.asyncio
async def test_smtp_worker_keeps_loop_responsive_and_drains_cancelled_operation():
    worker = MailWorker()
    started, finish = threading.Event(), threading.Event()
    thread_ids = []

    def send():
        thread_ids.append(threading.get_ident())
        started.set()
        assert finish.wait(5)
        return "accepted"

    task = asyncio.create_task(worker.run(send))
    try:
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(.01)
        assert started.is_set()
        task.cancel()
        await asyncio.sleep(.01)
        assert not task.done()
        finish.set()
        assert await task == "accepted"
        assert worker.cancel_requested
        await worker.run(lambda: thread_ids.append(threading.get_ident()))
        assert len(set(thread_ids)) == 1
        assert thread_ids[0] != threading.get_ident()
    finally:
        finish.set()
        await task
        worker.close()


@pytest.mark.parametrize("zipped", [False, True])
def test_html_and_zip_extract_regular_and_obfuscated_addresses(zipped):
    from emailbot.utils.file_email_extractor import extract_emails_from_bytes
    from emailbot.run_control import clear_stop

    clear_stop()
    content = b"<p>person@example.com</p><p>author [at] uni [dot] ru</p>"
    filename = "contacts.html"
    if zipped:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            archive.writestr(filename, content)
        content, filename = output.getvalue(), "contacts.zip"
    addresses, _, _ = extract_emails_from_bytes(content, filename)
    assert set(addresses) == {"person@example.com", "author@uni.ru"}


@pytest.mark.parametrize("session_api", [False, True])
def test_failed_connection_releases_batch_reservation(monkeypatch, session_api):
    from email.message import EmailMessage
    from emailbot import messaging as module
    from emailbot.smtp_client import DeliveryUncertainError

    msg = EmailMessage()
    msg["From"], msg["To"] = "sender@example.com", "person@example.com"
    msg.set_content("Hello", subtype="html")
    monkeypatch.setattr(module, "_sent_idempotency", set())
    monkeypatch.setattr(module, "_inflight_idempotency", set())
    monkeypatch.setattr(module, "_is_blocklisted", lambda _: False)
    monkeypatch.setattr(module, "decide", lambda *a: (module.Decision.SEND_NOW, "ok"))
    monkeypatch.setattr(module, "build_message", lambda *a, **k: (msg, "token"))
    monkeypatch.setattr(module, "was_sent_today_same_content", lambda *a: False)
    monkeypatch.setattr(module, "log_sent_email", Mock(return_value="key"))
    monkeypatch.setattr(module, "save_to_sent_folder", Mock())
    monkeypatch.setattr(module.ledger, "record_send", Mock())
    transport = Mock(side_effect=[ConnectionRefusedError("before SMTP"), True])
    monkeypatch.setattr(module, "send_raw_smtp_with_retry", transport)
    client = NS(send=transport)

    def send(batch):
        if session_api:
            return module.send_email_with_sessions(client, None, None, "person@example.com", "test.html", batch_id=batch)[0]
        return module.send_email("person@example.com", "test.html", batch_id=batch)

    with pytest.raises(ConnectionRefusedError):
        send("retry")
    assert send("retry") == module.SendOutcome.SENT
    assert send("retry") == module.SendOutcome.COOLDOWN
    assert transport.call_count == 2
    transport.side_effect = DeliveryUncertainError("lost response after DATA")
    try:
        send("uncertain")
    except DeliveryUncertainError:
        pass
    assert send("uncertain") == module.SendOutcome.COOLDOWN
    assert transport.call_count == 3
    if session_api:
        transport.side_effect = TypeError("failure inside send, not a signature mismatch")
        assert send("type-error") == module.SendOutcome.ERROR
        assert transport.call_count == 4


@pytest.mark.parametrize("port,use_ssl", [(465, True), (587, False)])
def test_robust_smtp_infers_tls_from_port(monkeypatch, port, use_ssl):
    from emailbot.smtp_client import RobustSMTP

    monkeypatch.setenv("SMTP_PORT", str(port))
    monkeypatch.delenv("SMTP_SSL", raising=False)
    assert RobustSMTP().ssl is use_ssl


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [0, 1])
@pytest.mark.parametrize("separate_chat", [False, True])
async def test_bulk_quota_preserves_pending_and_resumes_without_replaying(tmp_path, monkeypatch, available, separate_chat):
    import copy
    from emailbot.handlers import manual_send as handler

    template = tmp_path / "test.html"
    template.write_text("Hello", encoding="utf-8")
    state = {"pending": ["alice@example.com", "bob@example.com"], "group": "test", "template": str(template)}
    persisted = {42: state}
    monkeypatch.setattr(handler.mass_state, "load_chat_state", lambda chat: copy.deepcopy(persisted.get(chat)))
    monkeypatch.setattr(handler.mass_state, "save_chat_state", lambda chat, value: persisted.__setitem__(chat, copy.deepcopy(value)))
    monkeypatch.setattr(handler.mass_state, "clear_chat_state", lambda chat: persisted.pop(chat, None))
    bot = Mock()
    bot.is_force_send.return_value = False
    monkeypatch.setattr(handler, "_bot_handlers", lambda: bot)
    for name in ("_bulk_audit_api", "_cooldown_api"):
        monkeypatch.setattr(handler, name, lambda: None)
    for name in ("is_foreign", "is_suppressed", "should_stop"):
        monkeypatch.setattr(handler, name, lambda *a: False)
    monkeypatch.setattr(handler.blocked_domains, "is_blocked_email_domain", lambda _: False)
    monkeypatch.setattr(handler, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(handler, "get_sent_today", lambda: set())
    monkeypatch.setattr(handler, "MAX_EMAILS_PER_DAY", available)
    for name in ("log_sent_email", "clear_recent_sent_cache", "start_delivery_watch"):
        monkeypatch.setattr(handler, name, Mock())
    monkeypatch.setattr(handler, "safe_answer", AsyncMock())
    monkeypatch.setattr(handler, "notify", AsyncMock())
    monkeypatch.setattr(handler, "check_batch_delivery", AsyncMock(return_value=({}, "")))
    monkeypatch.setattr(handler, "RobustSMTP", Mock())
    original_sleep = asyncio.sleep

    async def fast_sleep(_):
        await original_sleep(0)

    monkeypatch.setattr(handler.asyncio, "sleep", fast_sleep)
    sent = []

    def send(*args, **kwargs):
        sent.append(args[3])
        return handler.SendOutcome.SENT, "token", "key", "hash"

    monkeypatch.setattr(handler, "send_email_with_sessions", send)
    message = NS(chat=NS(id=42), reply_text=AsyncMock())
    update = NS(callback_query=NS(message=message))
    context = NS(user_data={"ignore_180d": True}, chat_data={"history_snapshot": {
        "frozen_to_send": state["pending"],
        "frozen_original_map": {email: email for email in state["pending"]},
    }})
    task = await handler.send_all(update, context)
    if separate_chat:
        persisted[43] = copy.deepcopy(state)
        other_message = NS(chat=NS(id=43), reply_text=AsyncMock())
        other_context = NS(user_data={"ignore_180d": True}, chat_data={})
        other = await handler.send_all(NS(callback_query=NS(message=other_message)), other_context)
        assert task.cancelling() == 0
        await other
    await task
    assert persisted[42]["pending"] == state["pending"][available:]
    assert sent.count("alice@example.com") == available * (2 if separate_chat else 1)
    assert "bob@example.com" not in sent
    monkeypatch.setattr(handler, "MAX_EMAILS_PER_DAY", 10)
    await (await handler.send_all(update, context))
    assert sent.count("alice@example.com") == 1 + int(separate_chat and available == 1)
    assert sent.count("bob@example.com") == 1
    assert 42 not in persisted
