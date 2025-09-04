import asyncio
import csv
import logging
import smtplib
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from aiohttp import web
import pytest

from emailbot import messaging
from emailbot import unsubscribe
from emailbot import messaging_utils as mu
from emailbot.reporting import build_mass_report_text


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
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    ts = datetime.fromisoformat(row["last_sent_at"])
    assert abs((datetime.utcnow() - ts).total_seconds()) < 5
    assert row["email"] == "user@example.com"
    assert row["source"] == "group1"
    assert row["status"] == "ok"


def test_mass_report_has_no_addresses():
    text = build_mass_report_text(
        ["a@example.com"],
        ["b@example.com"],
        ["c@example.de"],
        ["d@example.com"],
    )
    assert "@" not in text
    assert "✅ Отправлено: 1" in text
    assert "⏳ Пропущены (<180 дней): 1" in text
    assert "🚫 В блок-листе/недоступны: 1" in text
    assert "🌍 Иностранные (отложены): 1" in text


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


def test_build_message_logo_toggle(tmp_path, monkeypatch):
    html_file = tmp_path / "template.html"
    html_file.write_text(
        "<html><body><img src=\"cid:logo\" alt=\"l\">Hello</body></html>",
        encoding="utf-8",
    )
    logo = tmp_path / "Logo.png"
    logo.write_bytes(b"fake")
    monkeypatch.setattr(messaging, "SCRIPT_DIR", tmp_path)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    monkeypatch.setenv("INLINE_LOGO", "1")
    msg, _ = messaging.build_message(
        "r@example.com", str(html_file), "Subj"
    )
    html = msg.get_body("html").get_content()
    assert html.count("cid:logo") == 1
    assert html.count("<img") == 1

    monkeypatch.setenv("INLINE_LOGO", "0")
    msg2, _ = messaging.build_message(
        "r@example.com", str(html_file), "Subj"
    )
    html2 = msg2.get_body("html").get_content()
    assert "cid:logo" not in html2
    assert "<img" not in html2


@pytest.mark.parametrize("template_name", ["sport.html", "tourism.html", "medicine.html"])
def test_repository_templates_logo_toggle(monkeypatch, template_name):
    template_path = Path(__file__).resolve().parents[1] / "templates" / template_name
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    monkeypatch.setenv("INLINE_LOGO", "1")
    msg, _ = messaging.build_message("u@example.com", str(template_path), "Subj")
    html = msg.get_body("html").get_content()
    assert html.count("cid:logo") == 1
    assert html.count("<img") == 1

    monkeypatch.setenv("INLINE_LOGO", "0")
    msg2, _ = messaging.build_message("u@example.com", str(template_path), "Subj")
    html2 = msg2.get_body("html").get_content()
    assert "cid:logo" not in html2
    assert "<img" not in html2


def test_count_sent_today_ignores_external(tmp_path, monkeypatch):
    log = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    with open(log, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "email", "last_sent_at", "source", "status"])
        w.writeheader()
        w.writerow({
            "key": mu.canonical_for_history("a@example.com"),
            "email": "a@example.com",
            "last_sent_at": today.isoformat(),
            "source": "g",
            "status": "external",
        })
        w.writerow({
            "key": mu.canonical_for_history("b@example.com"),
            "email": "b@example.com",
            "last_sent_at": today.isoformat(),
            "source": "g",
            "status": "ok",
        })
        w.writerow({
            "key": mu.canonical_for_history("c@example.com"),
            "email": "c@example.com",
            "last_sent_at": yesterday.isoformat(),
            "source": "g",
            "status": "ok",
        })
    assert messaging.count_sent_today() == 1
    assert messaging.get_sent_today() == {"b@example.com"}


def test_limit_not_triggered_by_external(tmp_path, monkeypatch):
    log = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    today = datetime.utcnow()
    with open(log, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "email", "last_sent_at", "source", "status"])
        w.writeheader()
        for i in range(3000):
            email = f"ext{i}@e.com"
            w.writerow({
                "key": mu.canonical_for_history(email),
                "email": email,
                "last_sent_at": today.isoformat(),
                "source": "g",
                "status": "external",
            })
    sent_today = messaging.get_sent_today()
    available = max(0, messaging.MAX_EMAILS_PER_DAY - len(sent_today))
    assert available == messaging.MAX_EMAILS_PER_DAY


def test_limit_triggered_after_200_sent(tmp_path, monkeypatch):
    log = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    today = datetime.utcnow()
    with open(log, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "email", "last_sent_at", "source", "status"])
        w.writeheader()
        for i in range(200):
            email = f"ok{i}@e.com"
            w.writerow({
                "key": mu.canonical_for_history(email),
                "email": email,
                "last_sent_at": today.isoformat(),
                "source": "g",
                "status": "ok",
            })
    sent_today = messaging.get_sent_today()
    available = max(0, messaging.MAX_EMAILS_PER_DAY - len(sent_today))
    assert available == 0


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
        row = next(csv.DictReader(f))
    assert row["unsubscribed"] == "1" and row["unsubscribe_token"] == "tok123"


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
        row = next(csv.DictReader(f))
    assert row["unsubscribed"] == "1"


def test_send_email_idempotent(tmp_path, monkeypatch):
    html = tmp_path / "t.html"
    html.write_text("<html><body>Hi</body></html>", encoding="utf-8")
    sent: list[str] = []

    class DummySmtp:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def send(self, *a, **kw):
            sent.append("1")

    monkeypatch.setattr(messaging, "SmtpClient", DummySmtp)
    monkeypatch.setattr(messaging, "save_to_sent_folder", lambda *a, **k: None)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "s@example.com")

    messaging.send_email("u@example.com", str(html), batch_id="b1")
    messaging.send_email("u@example.com", str(html), batch_id="b1")
    assert len(sent) == 1


def test_domain_rate_limit(monkeypatch):
    times = [0, 0]

    def fake_monotonic():
        return times.pop(0)

    sleeps: list[float] = []

    def fake_sleep(sec):
        sleeps.append(sec)

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
    monkeypatch.setattr(messaging.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(messaging.time, "sleep", fake_sleep)
    messaging._last_domain_send.clear()

    messaging.send_raw_smtp_with_retry("m", "a@example.com")
    messaging.send_raw_smtp_with_retry("m", "b@example.com")
    assert sleeps and sleeps[0] >= messaging._DOMAIN_RATE_LIMIT


def test_soft_bounce_retry_and_no_suppress(monkeypatch, caplog):
    calls = {"n": 0}

    class DummySmtp:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def send(self, *a, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise smtplib.SMTPResponseException(451, b"try again")

    sleeps: list[float] = []
    monkeypatch.setattr(messaging.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(messaging, "SmtpClient", DummySmtp)
    monkeypatch.setattr(messaging, "save_to_sent_folder", lambda *a, **k: None)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "s@example.com")
    monkeypatch.setattr(messaging, "_rate_limit_domain", lambda r: None)

    suppressed: list[str] = []
    monkeypatch.setattr(mu, "suppress_add", lambda *a, **k: suppressed.append("x"))

    with caplog.at_level(logging.INFO):
        messaging.send_raw_smtp_with_retry("m", "a@example.com", max_tries=2)

    assert calls["n"] == 2
    assert suppressed == []
    assert sleeps and sleeps[0] == 1
    assert any("Soft bounce" in rec.message for rec in caplog.records)


def test_hard_bounce_triggers_suppress(monkeypatch):
    class DummySmtp:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def send(self, *a, **kw):
            raise smtplib.SMTPResponseException(550, b"user unknown")

    monkeypatch.setattr(messaging, "SmtpClient", DummySmtp)
    monkeypatch.setattr(messaging, "_rate_limit_domain", lambda r: None)
    monkeypatch.setattr(messaging.time, "sleep", lambda s: None)

    called: list[str] = []
    monkeypatch.setattr(messaging, "suppress_add", lambda *a, **k: called.append("x"))
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "s@example.com")

    with pytest.raises(smtplib.SMTPResponseException):
        messaging.send_raw_smtp_with_retry("m", "a@example.com", max_tries=1)
    assert called == ["x"]
