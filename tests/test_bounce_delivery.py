from __future__ import annotations

import email
import json
from datetime import datetime, timezone
from email.header import Header
from unittest.mock import AsyncMock, Mock

import pytest

from utils import bounce, bounce_pop3, send_stats
from utils.bounce_common import extract_bounces, extract_recipient_fallback, extract_original_message
from utils.bounce_processing import record_bounce_message
from emailbot import delivery_monitor, messaging_utils


def dsn(*, action="failed", status="5.0.0", recipient="bad@example.com", mid="bounce-1", extra=""):
    subject = Header("Ваше сообщение не доставлено. Mail failure.", "utf-8").encode()
    return (f"From: MAILER-DAEMON@corp.mail.ru\r\n"
        f"To: sender@example.com\r\nSubject: {subject}\r\n"
        f"Message-ID: <{mid}@mail.ru>\r\nDate: Fri, 06 Nov 2026 11:15:00 +0300\r\n"
        "MIME-Version: 1.0\r\nContent-Type: multipart/report; report-type=delivery-status; boundary=dsn\r\n\r\n"
        "--dsn\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nDelivery failed\r\n"
        "--dsn\r\nContent-Type: message/delivery-status\r\n\r\nReporting-MTA: dns; mail.ru\r\n\r\n"
        f"Final-Recipient: rfc822;{recipient}\r\nAction: {action}\r\nStatus: {status}\r\n"
        "Diagnostic-Code: smtp; 550 5.1.1 No such user!\r\n"
        f"{extra}\r\n--dsn\r\nContent-Type: text/rfc822-headers\r\n\r\n"
        "From: sender@example.com\r\nTo: Other Name <other@example.com>\r\n"
        "Date: Fri, 06 Nov 2026 11:10:00 +0300\r\nMessage-ID: <original@example.com>\r\n"
        "X-EBOT-UUID: uuid-1\r\n\r\n--dsn--\r\n").encode()


@pytest.fixture
def isolated_logs(monkeypatch, tmp_path):
    monkeypatch.setenv("SEND_STATS_PATH", str(tmp_path / "stats.jsonl"))
    monkeypatch.setattr(messaging_utils, "BOUNCE_LOG_PATH", tmp_path / "bounces.csv")
    monkeypatch.setattr(messaging_utils, "SUPPRESS_PATH", tmp_path / "suppress.csv")
    monkeypatch.setenv("EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.setenv("EMAIL_PASSWORD", "test-password")
    monkeypatch.setenv("BOUNCE_FETCH_BACKEND", "imap")
    bounce._READ_UIDS.clear()
    return tmp_path


def test_mailru_nested_status_and_headers_only_original():
    msg = email.message_from_bytes(dsn())
    rows = extract_bounces(msg)
    assert len(rows) == 1
    row = rows[0]
    assert row.recipient == "bad@example.com"
    assert row.status == "5.0.0"
    assert row.action == "failed"
    assert row.reason == "smtp; 550 5.1.1 No such user!"
    assert row.message_id == "<original@example.com>"
    assert row.uuid == "uuid-1"
    assert row.original_date == datetime(2026, 11, 6, 8, 10, tzinfo=timezone.utc)
    assert extract_recipient_fallback(msg) == "bad@example.com"
    assert extract_original_message(msg)["Message-ID"] == row.message_id


@pytest.mark.parametrize("action,status", [("delivered", "2.0.0"), ("relayed", "2.0.0"), ("expanded", "2.0.0")])
def test_successful_delivery_report_is_not_an_error(action, status):
    assert extract_bounces(email.message_from_bytes(dsn(action=action, status=status))) == []


def test_multiple_dsn_recipients_and_diagnostic_unfolding():
    extra = ("\r\nFinal-Recipient: rfc822;second@example.com\r\nAction: failed\r\n"
             "Status: 5.1.1\r\nDiagnostic-Code: smtp; 550-5.1.1 Does not exist\r\n 550 Try again")
    rows = extract_bounces(email.message_from_bytes(dsn(extra=extra)))
    assert [r.recipient for r in rows] == ["bad@example.com", "second@example.com"]
    assert rows[1].reason == "smtp; 550-5.1.1 Does not exist 550 Try again"


def test_never_records_the_senders_own_mailbox():
    msg = email.message_from_bytes(b"From: mailer-daemon@example.com\r\nTo: sender@example.com\r\n\r\nFailure")
    assert extract_bounces(msg) == []
    ordinary = email.message_from_bytes(b"From: person@example.com\r\nTo: sender@example.com\r\n\r\nHello")
    assert extract_bounces(ordinary) == []


def test_recording_is_durable_and_idempotent(isolated_logs):
    assert record_bounce_message(dsn()) == 1
    assert record_bounce_message(dsn()) == 0
    rows = [json.loads(line) for line in (isolated_logs / "stats.jsonl").read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["status"] == "bounce"
    assert rows[0]["original_sent_at"] == "2026-11-06T08:10:00+00:00"
    assert "bad@example.com" in (isolated_logs / "suppress.csv").read_text()
    assert "delivery" in (isolated_logs / "bounces.csv").read_text()
    assert "sender@example.com" not in (isolated_logs / "suppress.csv").read_text()


def test_temporary_failure_is_recorded_without_permanent_suppression(isolated_logs):
    assert record_bounce_message(dsn(action="delayed", status="4.2.2")) == 1
    assert not (isolated_logs / "suppress.csv").exists()


def fake_imap(raw):
    client = Mock()
    client.select.return_value = ("OK", [])
    client.response.return_value = ("UIDVALIDITY", [b"1"])
    def uid(command, *args):
        if command == "search":
            return "OK", [b"42"]
        assert command == "fetch"
        assert args == (b"42", "(BODY.PEEK[])")
        return "OK", [b"FLAGS ()", (b"42 BODY[]", raw), b")"]
    client.uid.side_effect = uid
    return client


def test_imap_preserves_flags_uses_uids_and_deduplicates_after_restart(isolated_logs, monkeypatch):
    client = fake_imap(dsn())
    monkeypatch.setattr(bounce, "try_imap_connect", lambda: client)
    assert bounce.sync_bounces() == 1
    bounce._READ_UIDS.clear()  # A process restart or moving the DSN back into INBOX.
    assert bounce.sync_bounces() == 0
    assert client.select.call_args.kwargs == {"readonly": True}
    assert client.logout.call_count == 2
    client.store.assert_not_called()
    client.expunge.assert_not_called()


def test_imap_failure_is_visible_and_session_is_closed(isolated_logs, monkeypatch):
    client = fake_imap(dsn())
    client.select.return_value = ("NO", [])
    monkeypatch.setattr(bounce, "try_imap_connect", lambda: client)
    with pytest.raises(RuntimeError, match="select"):
        bounce.sync_bounces()
    client.logout.assert_called_once()


def test_explicit_pop3_backend_does_not_try_imap(isolated_logs, monkeypatch):
    monkeypatch.setenv("BOUNCE_FETCH_BACKEND", "pop3")
    monkeypatch.setattr(bounce, "try_imap_connect", Mock(side_effect=AssertionError("no IMAP")))
    monkeypatch.setattr(bounce, "sync_bounces_pop3", lambda: 4)
    assert bounce.sync_bounces() == 4


def test_pop3_uses_the_same_parser_and_never_deletes(isolated_logs, monkeypatch):
    client = Mock()
    client.list.return_value = (b"+OK", [b"1 100"], 100)
    client.top.return_value = (b"+OK", dsn().split(b"\r\n\r\n", 1)[0].split(b"\r\n"), 100)
    client.retr.return_value = (b"+OK", dsn().split(b"\r\n"), 100)
    monkeypatch.setattr(bounce_pop3.poplib, "POP3_SSL", lambda *a, **k: client)
    monkeypatch.setenv("BOUNCE_SINCE_DAYS", "0")
    assert bounce_pop3.sync_bounces_pop3() == 1
    assert bounce_pop3.sync_bounces_pop3() == 0
    client.dele.assert_not_called()
    assert client.quit.call_count == 2


def test_imap_only_fetches_server_selected_candidates(isolated_logs, monkeypatch):
    client = fake_imap(dsn())
    monkeypatch.setattr(bounce, "try_imap_connect", lambda: client)
    assert bounce.sync_bounces() == 1
    search = client.uid.call_args_list[0].args
    assert search[:3] == ("search", None, "SINCE")
    assert search[4:] == (
        "OR", "OR", "FROM", '"mailer-daemon"', "FROM", '"postmaster"',
        "OR", "SUBJECT", '"failure"', "SUBJECT", '"undelivered"',
    )
    assert "UNSEEN" not in search
    assert "SEEN" not in search
    # Other unread messages are not in the filtered server response.
    assert client.uid.call_args_list[1].args == ("fetch", b"42", "(BODY.PEEK[])")
    assert client.uid.call_count == 2


def test_ordinary_message_with_failure_subject_is_not_counted(isolated_logs, monkeypatch):
    raw = (b"From: colleague@example.com\r\nTo: sender@example.com\r\n"
           b"Subject: Discussing a mail failure\r\n\r\nPlease check this issue.")
    client = fake_imap(raw)
    monkeypatch.setattr(bounce, "try_imap_connect", lambda: client)
    assert bounce.sync_bounces() == 0
    assert not (isolated_logs / "stats.jsonl").exists()
    assert not (isolated_logs / "bounces.csv").exists()
    assert not (isolated_logs / "suppress.csv").exists()


def test_ordinary_failure_discussion_with_attached_original_is_not_counted(isolated_logs):
    raw = (b"From: colleague@example.com\r\nTo: sender@example.com\r\n"
           b"Subject: Discussing a mail failure\r\nMIME-Version: 1.0\r\n"
           b"Content-Type: multipart/mixed; boundary=discussion\r\n\r\n"
           b"--discussion\r\nContent-Type: text/plain\r\n\r\nPlease check the attached email.\r\n"
           b"--discussion\r\nContent-Type: message/rfc822\r\n\r\n"
           b"From: sender@example.com\r\nTo: innocent@example.com\r\n\r\nHello\r\n"
           b"--discussion--\r\n")
    assert record_bounce_message(raw) == 0
    assert not (isolated_logs / "stats.jsonl").exists()
    assert not (isolated_logs / "suppress.csv").exists()


def test_pop3_downloads_only_bounce_bodies(isolated_logs, monkeypatch):
    ordinary = b"From: colleague@example.com\r\nSubject: Meeting\r\n"
    candidate = dsn().split(b"\r\n\r\n", 1)[0]
    client = Mock()
    client.list.return_value = (b"+OK", [b"1 100", b"2 100", b"3 100"], 300)
    client.top.side_effect = [
        (b"+OK", ordinary.split(b"\r\n"), 100),
        (b"+OK", candidate.split(b"\r\n"), 100),
        (b"+OK", ordinary.split(b"\r\n"), 100),
    ]
    client.retr.return_value = (b"+OK", dsn().split(b"\r\n"), 100)
    monkeypatch.setattr(bounce_pop3.poplib, "POP3_SSL", lambda *a, **k: client)
    monkeypatch.setenv("BOUNCE_SINCE_DAYS", "0")
    assert bounce_pop3.sync_bounces_pop3() == 1
    assert [call.args for call in client.top.call_args_list] == [(1, 0), (2, 0), (3, 0)]
    client.retr.assert_called_once_with(2)
    client.dele.assert_not_called()


def test_pop3_headers_can_identify_dsn_from_another_sender(isolated_logs, monkeypatch):
    raw = dsn().replace(b"MAILER-DAEMON@corp.mail.ru", b"delivery@provider.example")
    # MIME report-type must suffice even without the familiar subject.
    raw = raw.replace(raw.split(b"Subject: ", 1)[1].split(b"\r\n", 1)[0], b"Notification")
    client = Mock()
    client.list.return_value = (b"+OK", [b"1 100"], 100)
    client.top.return_value = (b"+OK", raw.split(b"\r\n\r\n", 1)[0].split(b"\r\n"), 100)
    client.retr.return_value = (b"+OK", raw.split(b"\r\n"), 100)
    monkeypatch.setattr(bounce_pop3.poplib, "POP3_SSL", lambda *a, **k: client)
    monkeypatch.setenv("BOUNCE_SINCE_DAYS", "0")
    assert bounce_pop3.sync_bounces_pop3() == 1


def test_pop3_does_not_download_bodies_when_top_is_unsupported(isolated_logs, monkeypatch):
    import poplib
    client = Mock()
    client.list.return_value = (b"+OK", [b"1 100"], 100)
    client.top.side_effect = poplib.error_proto(b"-ERR TOP unsupported")
    monkeypatch.setattr(bounce_pop3.poplib, "POP3_SSL", lambda *a, **k: client)
    with pytest.raises(poplib.error_proto, match="TOP unsupported"):
        bounce_pop3.sync_bounces_pop3()
    client.retr.assert_not_called()
    client.quit.assert_called_once()


def test_auto_fallback_on_imap_login_failure(isolated_logs, monkeypatch):
    import imaplib
    monkeypatch.setenv("BOUNCE_FETCH_BACKEND", "auto")
    client = fake_imap(dsn())
    client.login.side_effect = imaplib.IMAP4.error("unavailable")
    monkeypatch.setattr(bounce, "try_imap_connect", lambda: client)
    monkeypatch.setattr(bounce, "sync_bounces_pop3", lambda: 2)
    assert bounce.sync_bounces() == 2
    client.logout.assert_called_once()


def test_background_scan_retries_errors_and_stops_promptly(monkeypatch):
    stop = Mock()
    stop.is_set.side_effect = [False, False, True]
    scan = Mock(side_effect=[OSError("offline"), 2])
    monkeypatch.setattr(bounce, "sync_bounces", scan)
    monkeypatch.setenv("BOUNCE_CHECK_INTERVAL_SECONDS", "60")
    bounce.periodic_bounce_check(stop)
    assert scan.call_count == 2
    assert stop.wait.call_args_list == [((60,),), ((60,),)]


def test_batch_matching_excludes_old_original_and_other_recipients(isolated_logs):
    record_bounce_message(dsn())
    started = datetime(2026, 11, 6, 8, 9, tzinfo=timezone.utc)
    assert set(delivery_monitor.batch_bounces(["bad@example.com"], started)) == {"bad@example.com"}
    assert delivery_monitor.batch_bounces(["other@example.com"], started) == {}
    # A late DSN for an older send must not taint a new mailing.
    later = datetime(2026, 11, 6, 8, 12, tzinfo=timezone.utc)
    assert delivery_monitor.batch_bounces(["bad@example.com"], later) == {}


def test_batch_matching_keeps_original_gmail_address(isolated_logs):
    record_bounce_message(dsn(recipient="First.Last+tag@gmail.com"))
    started = datetime(2026, 11, 6, 8, 9, tzinfo=timezone.utc)
    assert set(delivery_monitor.batch_bounces(["First.Last+tag@gmail.com"], started)) == {"first.last+tag@gmail.com"}


def test_bounce_cache_reuses_unchanged_log_and_sees_new_failures(isolated_logs, monkeypatch):
    from pathlib import Path
    record_bounce_message(dsn())
    real_open = Path.open
    reads = []
    def tracking_open(path, *args, **kwargs):
        if path == isolated_logs / "stats.jsonl":
            reads.append(path)
        return real_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", tracking_open)
    started = datetime(2026, 11, 6, 8, 9, tzinfo=timezone.utc)
    assert len(delivery_monitor.batch_bounces(["bad@example.com"], started)) == 1
    assert len(delivery_monitor.batch_bounces(["bad@example.com"], started)) == 1
    assert len(reads) == 1
    record_bounce_message(dsn(recipient="second@example.com", mid="bounce-2"))
    assert len(delivery_monitor.batch_bounces(["bad@example.com", "second@example.com"], started)) == 2


@pytest.mark.asyncio
async def test_late_watch_keeps_updating_after_thirty_minutes(monkeypatch):
    monkeypatch.delenv("BOUNCE_WATCH_SECONDS", raising=False)
    monkeypatch.setenv("BOUNCE_CHECK_INTERVAL_SECONDS", "1801")
    clock = [0.0]
    monkeypatch.setattr(delivery_monitor.asyncio, "get_running_loop", lambda: Mock(time=lambda: clock[0]))
    async def advance(seconds):
        clock[0] += seconds
    monkeypatch.setattr(delivery_monitor.asyncio, "sleep", advance)
    calls = [0]
    def failures(*_):
        calls[0] += 1
        if calls[0] > 1:
            clock[0] = 604800
        return {"bad@example.com": {"reason": "550"}}
    monkeypatch.setattr(delivery_monitor, "batch_bounces", failures)
    summary = Mock(edit_text=AsyncMock())
    monkeypatch.setattr(delivery_monitor, "notify", AsyncMock())
    await delivery_monitor._watch(Mock(), ["bad@example.com"], datetime.now(timezone.utc), set(), None,
                                  summary_message=summary, summary_text="✅ Отправлено: 1")
    summary.edit_text.assert_awaited_once_with("✅ Отправлено: 0\n📬 Недоставляемые: 1")
    assert calls[0] == 2


@pytest.mark.asyncio
async def test_failed_post_send_check_does_not_claim_delivery(isolated_logs, monkeypatch):
    monkeypatch.setattr(delivery_monitor, "sync_bounces", Mock(side_effect=OSError("offline")))
    rows, warning = await delivery_monitor.check_batch_delivery([], datetime.now(timezone.utc))
    assert rows == {}
    assert "недоступна" in warning


@pytest.mark.asyncio
@pytest.mark.parametrize("first_edit_fails", [False, True])
@pytest.mark.parametrize("notice_fails", [False, True])
async def test_late_failure_notified_once_and_added_to_audit(monkeypatch, first_edit_fails, notice_fails):
    monkeypatch.setenv("BOUNCE_CHECK_INTERVAL_SECONDS", "10")
    monkeypatch.setenv("BOUNCE_WATCH_SECONDS", "30")
    clock = [0.0]
    loop = Mock(time=lambda: clock[0])
    monkeypatch.setattr(delivery_monitor.asyncio, "get_running_loop", lambda: loop)
    async def advance(seconds):
        clock[0] += seconds
    monkeypatch.setattr(delivery_monitor.asyncio, "sleep", advance)
    monkeypatch.setattr(delivery_monitor, "batch_bounces", lambda *a: {"bad@example.com": {"reason": "550 No such user"}})
    notify = AsyncMock(side_effect=OSError("temporary notice failure") if notice_fails else None)
    monkeypatch.setattr(delivery_monitor, "notify", notify)
    audit = Mock()
    summary = Mock(edit_text=AsyncMock(side_effect=[OSError("temporary"), None] if first_edit_fails else None))
    await delivery_monitor._watch(Mock(), ["bad@example.com"], datetime.now(timezone.utc), set(), audit,
                                  summary_message=summary, summary_text="📨 Рассылка завершена.\n✅ Отправлено: 1")
    notify.assert_awaited_once()
    audit.log_error.assert_called_once()
    assert "с учётом возвратов: 0" in notify.call_args.args[1]
    assert summary.edit_text.await_count == (2 if first_edit_fails else 1)
    assert summary.edit_text.call_args.args[0] == "📨 Рассылка завершена.\n✅ Отправлено: 0\n📬 Недоставляемые: 1"


@pytest.mark.asyncio
async def test_late_summary_does_not_subtract_initial_returns_again(monkeypatch):
    monkeypatch.setenv("BOUNCE_CHECK_INTERVAL_SECONDS", "10")
    monkeypatch.setenv("BOUNCE_WATCH_SECONDS", "30")
    clock = [0.0]
    monkeypatch.setattr(delivery_monitor.asyncio, "get_running_loop", lambda: Mock(time=lambda: clock[0]))
    async def advance(seconds):
        clock[0] += seconds
    monkeypatch.setattr(delivery_monitor.asyncio, "sleep", advance)
    initial = {"initial@example.com": {"reason": "550"}}
    first = {**initial, "first@example.com": {"reason": "550"}}
    second = {**first, "second@example.com": {"reason": "550"}}
    monkeypatch.setattr(delivery_monitor, "batch_bounces", Mock(side_effect=[first, second, second]))
    notifier = AsyncMock()
    monkeypatch.setattr(delivery_monitor, "notify", notifier)
    summary = Mock(edit_text=AsyncMock())
    await delivery_monitor._watch(Mock(), [*second, "good@example.com"], datetime.now(timezone.utc),
                                  set(initial), None, summary_message=summary,
                                  summary_text="✅ Отправлено: 3\n📬 Недоставляемые: 1",
                                  sent_count=3, undeliverable_count=1)
    assert summary.edit_text.await_count == 2
    assert summary.edit_text.call_args.args[0] == "✅ Отправлено: 1\n📬 Недоставляемые: 3"
    assert notifier.await_count == 2


@pytest.mark.asyncio
async def test_notify_returns_sent_summary_for_late_updates(monkeypatch):
    import importlib
    notifications = importlib.import_module("emailbot.notify")
    monkeypatch.setattr(notifications, "NOTIFY_LEVEL", "full")
    sent_message = object()
    source = Mock(reply_text=AsyncMock(return_value=sent_message))
    assert await notifications.notify(source, "Summary", event="finish") is sent_message
