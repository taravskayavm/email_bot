import imaplib
from unittest.mock import Mock

import pytest

from emailbot.sent_copy import SentCopySession

RAW = b"From: sender@example.com\r\nTo: user@example.com\r\nMessage-ID: <copy-1@example.com>\r\n\r\nHello"


def connection():
    conn = Mock()
    conn.select.return_value = ("OK", [])
    conn.append.return_value = ("OK", [])
    conn.uid.return_value = ("OK", [b""])
    return conn


def test_reconnect_after_select_failure_reuses_new_session():
    old, new = connection(), connection()
    old.select.side_effect = OSError(10053, "aborted")
    connect = Mock(return_value=new)
    session = SentCopySession(old, connect, lambda _: "Sent")
    assert session.save(RAW)
    assert session.save(RAW.replace(b"copy-1", b"copy-2"))
    connect.assert_called_once()
    old.append.assert_not_called()
    assert new.append.call_count == 2
    assert session.failure_report() == ""


@pytest.mark.parametrize("found, expected_appends", [(b"123", 0), (b"", 1)])
def test_lost_append_response_checks_message_id_before_retry(found, expected_appends):
    old, new = connection(), connection()
    old.append.side_effect = imaplib.IMAP4.abort("response lost")
    new.uid.return_value = ("OK", [found])
    session = SentCopySession(old, lambda: new, lambda _: "Sent")
    assert session.save(RAW)
    new.uid.assert_called_once_with("SEARCH", None, "HEADER", "Message-ID", '"<copy-1@example.com>"')
    assert new.append.call_count == expected_appends


def test_failed_search_does_not_blindly_repeat_append():
    old, new = connection(), connection()
    old.append.side_effect = OSError("lost response")
    new.uid.return_value = ("NO", [b"search failed"])
    session = SentCopySession(old, lambda: new, lambda _: "Sent")
    assert not session.save(RAW)
    new.append.assert_not_called()
    assert session.failed_recipients == ["user@example.com"]
    assert "не подтверждено: 1" in session.failure_report()
    assert "user@example.com" in session.failure_report()


def test_rejected_append_is_not_reported_as_saved():
    old, new = connection(), connection()
    old.append.return_value = new.append.return_value = ("NO", [b"quota exceeded"])
    session = SentCopySession(old, lambda: new, lambda _: "Sent")
    assert not session.save(RAW)
    assert session.failed_recipients == ["user@example.com"]
    assert old.append.call_count == new.append.call_count == 1


def test_failed_connection_is_bounded_and_reported():
    connect = Mock(side_effect=OSError("offline"))
    session = SentCopySession(None, connect, lambda _: "Sent")
    assert not session.save(RAW)
    assert connect.call_count == 2
    assert session.connection is None
    assert "user@example.com" in session.failure_report()


def test_logout_failure_does_not_hide_report():
    conn = connection()
    conn.logout.side_effect = imaplib.IMAP4.abort("closed")
    session = SentCopySession(conn, Mock(), Mock())
    session.logout()
    session.logout()
    conn.shutdown.assert_called_once()
    assert session.connection is None


def test_copy_failure_keeps_smtp_success_and_does_not_resend(tmp_path, monkeypatch):
    from emailbot import messaging
    html = tmp_path / "template.html"
    html.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "_is_blocklisted", lambda _: False)
    monkeypatch.setattr(messaging, "decide", lambda *_: (messaging.Decision.SEND_NOW, "ok"))
    monkeypatch.setattr(messaging, "was_sent_today_same_content", lambda *a, **k: False)
    monkeypatch.setattr(messaging, "_register_send", lambda *a, **k: True)
    monkeypatch.setattr(messaging.ledger, "record_send", Mock())
    log_sent = Mock(return_value="saved-key")
    monkeypatch.setattr(messaging, "log_sent_email", log_sent)
    smtp = Mock()
    connect = Mock(side_effect=OSError("IMAP offline"))
    session = SentCopySession(None, connect, lambda _: "Sent")
    outcome, *_ = messaging.send_email_with_sessions(smtp, session, "Sent", "user@example.com", str(html))
    assert outcome is messaging.SendOutcome.SENT
    smtp.send.assert_called_once()
    log_sent.assert_called_once()
    assert session.failed_recipients == ["user@example.com"]


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_failed", [False, True])
async def test_bulk_report_includes_copy_failure_after_cleanup(tmp_path, monkeypatch, delivery_failed):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from emailbot.handlers import manual_send as handler

    failures = {"user@example.com": {"reason": "550 No such user"}} if delivery_failed else {}
    monkeypatch.setattr(handler, "check_batch_delivery", AsyncMock(return_value=(failures, "")))
    monkeypatch.setattr(handler, "_bulk_audit_api", lambda: None)

    html = tmp_path / "template.html"
    html.write_text("Hello", encoding="utf-8")
    saved = {"pending": ["user@example.com"], "group": "test", "template": str(html), "template_label": "Test",
             "started_at": "2026-11-06T08:09:00+00:00"}
    monkeypatch.setattr(handler.mass_state, "load_chat_state", lambda _: saved)
    monkeypatch.setattr(handler.mass_state, "save_chat_state", Mock())
    monkeypatch.setattr(handler.mass_state, "clear_chat_state", Mock())
    bot = Mock()
    bot.is_force_send.return_value = False
    monkeypatch.setattr(handler, "_bot_handlers", lambda: bot)
    monkeypatch.setattr(handler, "_bulk_audit_api", lambda: None)
    monkeypatch.setattr(handler, "_cooldown_api", lambda: {})
    monkeypatch.setattr(handler, "safe_answer", AsyncMock())
    monkeypatch.setattr(handler, "get_blocked_emails", lambda: set())
    monkeypatch.setattr(handler, "get_sent_today", lambda: set())
    monkeypatch.setattr(handler, "is_foreign", lambda _: False)
    monkeypatch.setattr(handler, "is_suppressed", lambda _: False)
    monkeypatch.setattr(handler.blocked_domains, "is_blocked_email_domain", lambda _: False)
    monkeypatch.setattr(handler, "should_stop", lambda: False)
    monkeypatch.setattr(handler, "clear_stop", lambda: None)
    monkeypatch.setattr(handler, "log_sent_email", Mock())
    monkeypatch.setattr(handler, "clear_recent_sent_cache", Mock())
    monkeypatch.setattr(handler.asyncio, "sleep", AsyncMock())
    smtp = Mock()
    monkeypatch.setattr(handler, "RobustSMTP", lambda: smtp)
    # Exercise the real copy-saving/reporting path with unavailable IMAP.
    monkeypatch.setattr(handler.imaplib, "IMAP4_SSL", Mock(side_effect=OSError("offline")))
    monkeypatch.setattr(handler, "get_preferred_sent_folder", lambda _: "Sent")

    def send(client, session, folder, recipient, template, **kwargs):
        assert not handler.messaging.save_to_sent_folder(RAW, imap=session, folder=folder)
        return handler.SendOutcome.SENT, "token", "key", "hash"

    monkeypatch.setattr(handler, "send_email_with_sessions", send)
    tasks = []
    monkeypatch.setattr(handler.messaging, "create_task_with_logging", lambda coro, *a, **k: tasks.append(coro))
    notify = AsyncMock()
    monkeypatch.setattr(handler, "notify", notify)
    message = SimpleNamespace(chat=SimpleNamespace(id=999), reply_text=AsyncMock())
    update = SimpleNamespace(callback_query=SimpleNamespace(message=message))
    context = SimpleNamespace(user_data={"ignore_180d": True}, chat_data={})
    await handler.send_all(update, context)
    assert len(tasks) == 1
    await tasks[0]
    assert handler.check_batch_delivery.call_args.args[1] == handler.datetime.fromisoformat(saved["started_at"])
    reports = [call.args[1] for call in notify.call_args_list if call.kwargs.get("event") == "finish"]
    assert len(reports) == 1
    assert "не подтверждено: 1" in reports[0]
    if delivery_failed:
        assert "Отправлено: 0" in reports[0]
        assert "Недоставляемые: 1" in reports[0]
    assert "user@example.com" in reports[0]
    smtp.close.assert_called_once()
