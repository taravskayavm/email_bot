import asyncio
import csv
from datetime import datetime

import aiohttp
from aiohttp import web
import pytest

from emailbot import messaging
from emailbot import unsubscribe


@pytest.fixture(autouse=True)
def fake_smtp(monkeypatch):
    class DummySmtp:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def send(self, *a, **kw):
            pass

    monkeypatch.setattr(messaging, "SmtpClient", DummySmtp)


@pytest.fixture
def temp_files(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    log = tmp_path / "logs" / "sent_log.csv"
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked))
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    return blocked, log


def test_add_blocked_email_handles_duplicates_and_invalid(temp_files):
    blocked, _ = temp_files
    # invalid email
    assert messaging.add_blocked_email("invalid") is False
    assert not blocked.exists()

    # add new email
    assert messaging.add_blocked_email("User@Example.COM ") is True
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # duplicate should not be added
    assert messaging.add_blocked_email("user@example.com") is False
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # numeric variant should also be treated as duplicate
    assert messaging.add_blocked_email("1user@example.com") is False
    assert blocked.read_text().splitlines() == ["user@example.com"]


def test_dedupe_blocked_file_removes_duplicates_and_variants(temp_files):
    blocked, _ = temp_files
    blocked.write_text(
        "\n".join(
            [
                "john@example.com",
                "John@example.com",
                "1john@example.com",
                "2john@example.com",
                "1jane@example.com",
                "1john@example.com",
            ]
        )
        + "\n"
    )
    messaging.dedupe_blocked_file()
    result = blocked.read_text().splitlines()
    assert result == ["jane@example.com", "john@example.com"]


def test_log_sent_email_records_entries(temp_files):
    _, log_path = temp_files
    messaging.log_sent_email("USER@example.com", "group1")
    messaging.log_sent_email(
        "USER@example.com", "group1", status="error", error_msg="boom"
    )
    with open(log_path, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 2
    assert len(rows[0]) == 10
    ts = datetime.fromisoformat(rows[0][0])
    assert abs((datetime.utcnow() - ts).total_seconds()) < 5
    assert rows[0][1:4] == ["user@example.com", "group1", "ok"]
    assert rows[1][3] == "error" and rows[1][6] == "boom"


def test_build_message_adds_signature_and_unsubscribe(tmp_path, monkeypatch):
    html_file = tmp_path / "template.html"
    html_file.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    token_host = "example.com"
    monkeypatch.setenv("HOST", token_host)
    msg, token = messaging.build_message(
        "recipient@example.com", str(html_file), "Subject"
    )
    assert token
    html_part = msg.get_body("html")
    assert html_part is not None
    html = html_part.get_content()
    assert "Hello" in html
    assert "С уважением" in html
    assert f"https://{token_host}/unsubscribe?email=recipient@example.com&token={token}" in html
    text_part = msg.get_body("plain")
    assert (
        f"Отписаться: https://{token_host}/unsubscribe?email=recipient@example.com&token={token}"
        in text_part.get_content()
    )


def test_save_to_sent_folder_serializes_string():
    class DummyImap:
        def __init__(self):
            self.append_args = None

        def select(self, folder):
            return "OK", []

        def append(self, folder, flags, internaldate, msg_bytes):
            self.append_args = (folder, flags, internaldate, msg_bytes)
            return "OK", []

    msg = messaging.EmailMessage()
    msg["From"] = "a@example.com"
    msg["To"] = "b@example.com"
    msg.set_content("Hello")
    raw = msg.as_string()
    imap = DummyImap()
    messaging.save_to_sent_folder(raw, imap=imap, folder="Sent")
    assert isinstance(imap.append_args[3], bytes)
    assert imap.append_args[3] == raw.encode("utf-8")


def test_save_to_sent_folder_serializes_email_message():
    class DummyImap:
        def __init__(self):
            self.append_args = None

        def select(self, folder):
            return "OK", []

        def append(self, folder, flags, internaldate, msg_bytes):
            self.append_args = (folder, flags, internaldate, msg_bytes)
            return "OK", []

    msg = messaging.EmailMessage()
    msg["From"] = "a@example.com"
    msg["To"] = "b@example.com"
    msg.set_content("Hi")
    imap = DummyImap()
    messaging.save_to_sent_folder(msg, imap=imap, folder="Sent")
    assert imap.append_args[3] == msg.as_bytes()


def test_mark_unsubscribed_updates_log(temp_files):
    _, log_path = temp_files
    messaging.log_sent_email(
        "user@example.com", "group1", unsubscribe_token="tok123"
    )
    assert messaging.mark_unsubscribed("user@example.com", "tok123")
    with open(log_path, encoding="utf-8") as f:
        row = next(csv.reader(f))
    assert row[8] == "1" and row[7] == "tok123"


async def _start_app(app):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://localhost:{port}"


@pytest.mark.asyncio
async def test_unsubscribe_flow(temp_files, monkeypatch):
    _, log_path = temp_files
    monkeypatch.setattr(messaging, "LOG_FILE", str(log_path))
    token = "tok123"
    messaging.log_sent_email("user@example.com", "g", unsubscribe_token=token)
    app = unsubscribe.create_app()
    runner, base = await _start_app(app)
    async with aiohttp.ClientSession() as session:
        resp = await session.get(
            f"{base}/unsubscribe?email=user@example.com&token={token}"
        )
        html = await resp.text()
        assert "Подтвердить отписку" in html
        resp_bad = await session.get(
            f"{base}/unsubscribe?email=user@example.com&token=bad"
        )
        assert "ответьте Unsubscribe" in (await resp_bad.text())
        resp2 = await session.post(
            f"{base}/unsubscribe", data={"email": "user@example.com", "token": token}
        )
        html2 = await resp2.text()
        assert "Вы отписаны" in html2
    await runner.cleanup()
    with open(log_path, encoding="utf-8") as f:
        row = next(csv.reader(f))
    assert row[8] == "1"
