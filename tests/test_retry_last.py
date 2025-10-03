import asyncio
import csv
import types
from email.message import EmailMessage
from datetime import datetime
import email.utils

import pytest

pytest.importorskip("emailbot.bot_handlers")

import emailbot.bot_handlers as bh
from emailbot import messaging_utils as mu, messaging


class DummyMessage:
    def __init__(self):
        self.replies: list[str] = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append(text)
        return self


class DummyUpdate:
    def __init__(self):
        self.message = DummyMessage()
        self.effective_chat = types.SimpleNamespace(id=123)


class DummyContext:
    def __init__(self):
        self.chat_data = {}
        self.user_data = {}


def run(coro):
    return asyncio.run(coro)


def setup_paths(tmp_path, monkeypatch):
    bounce = tmp_path / "b.csv"
    sent = tmp_path / "s.csv"
    suppress = tmp_path / "sup.csv"
    monkeypatch.setattr(mu, "BOUNCE_LOG_PATH", bounce)
    monkeypatch.setattr(mu, "SUPPRESS_PATH", suppress)
    monkeypatch.setattr(bh, "BOUNCE_LOG_PATH", bounce)
    monkeypatch.setattr(messaging, "LOG_FILE", sent)
    monkeypatch.setattr(bh.messaging, "LOG_FILE", sent)
    return bounce, sent, suppress


def write_bounce(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "email", "code", "msg", "phase"])
        for r in rows:
            w.writerow(r)


def test_retry_last_only_soft(monkeypatch, tmp_path):
    bounce, sent, suppress = setup_paths(tmp_path, monkeypatch)
    write_bounce(
        bounce,
        [
            ["2023-01-01", "old@example.com", "450", "temporary", "send"],
            ["2023-01-02", "soft@example.com", "450", "temporary", "send"],
            ["2023-01-02", "hard@example.com", "550", "user not found", "send"],
        ],
    )
    suppress.write_text(
        "email,code,reason,first_seen,last_seen,hits\nfoo@example.com,550,hard,1,1,1\n"
    )
    before = suppress.read_text()
    sent_addrs = []
    monkeypatch.setattr(
        bh.messaging, "send_raw_smtp_with_retry", lambda m, a, max_tries=3: sent_addrs.append(a)
    )
    update = DummyUpdate()
    ctx = DummyContext()
    run(bh.retry_last_command(update, ctx))
    assert sent_addrs == ["soft@example.com"]
    with open(sent, encoding="utf-8") as f:
        data = f.read()
    assert "soft@example.com" in data
    assert suppress.read_text() == before
    assert update.message.replies[-1] == "Повторно отправлено: 1"


def test_retry_last_no_soft(monkeypatch, tmp_path):
    bounce, sent, suppress = setup_paths(tmp_path, monkeypatch)
    write_bounce(bounce, [["2023-01-02", "hard@example.com", "550", "user not found", "send"]])
    sent_addrs = []
    monkeypatch.setattr(
        bh.messaging, "send_raw_smtp_with_retry", lambda m, a, max_tries=3: sent_addrs.append(a)
    )
    update = DummyUpdate()
    ctx = DummyContext()
    run(bh.retry_last_command(update, ctx))
    assert sent_addrs == []
    assert not sent.exists()
    assert update.message.replies[-1] == "Нет писем для ретрая"


def test_sync_uses_upsert_and_no_duplicates(monkeypatch, tmp_path):
    log = tmp_path / "sent_log.csv"
    seen = tmp_path / "seen.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    monkeypatch.setattr(mu, "SYNC_SEEN_EVENTS_PATH", seen)
    monkeypatch.setattr(messaging, "SYNC_SEEN_EVENTS_PATH", seen)

    msg = EmailMessage()
    msg["To"] = "User <u@example.com>"
    msg["Message-ID"] = "<id1>"
    msg["Date"] = email.utils.format_datetime(datetime.utcnow())
    raw = msg.as_bytes()

    class DummyImap:
        def login(self, *a, **k):
            pass

        def select(self, folder):
            return "OK", [None]

        def list(self, *a, **k):
            return "OK", [b"Sent"]

        def search(self, charset, criteria):
            return "OK", [b"1"]

        def fetch(self, num, what):
            return "OK", [(b"", raw)]

        def logout(self):
            pass

    monkeypatch.setattr(messaging, "imap_connect_ssl", lambda *a, **k: DummyImap())

    stats1 = messaging.sync_log_with_imap()
    stats2 = messaging.sync_log_with_imap()

    assert stats1["new_contacts"] == 1
    assert stats2["new_contacts"] == 0
    assert stats2["skipped_events"] == 1
