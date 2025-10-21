import asyncio
import csv
import hashlib
import logging
import smtplib
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import aiohttp
from aiohttp import web
import pytest

from emailbot import messaging
from emailbot import unsubscribe
from emailbot import messaging_utils as mu
from emailbot.reporting import build_mass_report_text
from emailbot.settings import REPORT_TZ


@pytest.fixture(autouse=True)
def fake_smtp(monkeypatch):
    class DummySMTP:
        def __init__(self, *a, **kw):
            self.sent = []

        def send(self, msg):
            self.sent.append(msg)

        def ensure(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(messaging, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(
        messaging,
        "send_with_retry",
        lambda smtp, msg, *, retries=2, backoff=1.0: smtp.send(msg),
    )


@pytest.fixture
def temp_files(tmp_path, monkeypatch):
    blocked = tmp_path / "blocked.txt"
    log = tmp_path / "logs" / "sent_log.csv"
    monkeypatch.setattr(messaging, "BLOCKED_FILE", str(blocked))
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    messaging.suppress_list.init_blocked(str(blocked))
    monkeypatch.setattr(unsubscribe, "BLOCKED_FILE", str(blocked), raising=False)
    return blocked, log


def test_add_blocked_email_handles_duplicates_and_invalid(temp_files):
    blocked, _ = temp_files
    # invalid email
    assert messaging.add_blocked_email("invalid") is False
    assert blocked.exists()
    assert blocked.read_text() == ""

    # add new email
    assert messaging.add_blocked_email("User@Example.COM ") is True
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # duplicate should not be added
    assert messaging.add_blocked_email("user@example.com") is False
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # numeric variant should also be treated as duplicate
    assert messaging.add_blocked_email("1user@example.com") is False
    assert blocked.read_text().splitlines() == ["user@example.com"]

    # IDNA normalization
    assert messaging.add_blocked_email("Тест@пример.рф") is True
    assert "тест@xn--e1afmkfd.xn--p1ai" in blocked.read_text().splitlines()


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
    assert ts.tzinfo is not None
    tz = ZoneInfo(REPORT_TZ)
    now = datetime.now(tz)
    assert abs((now.astimezone(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds()) < 5
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
    assert "✉️ Рассылка завершена." in text
    assert "📦 В очереди было: 4" in text
    assert "✅ Успешно отправлено: 1" in text
    assert "⏳ Пропущены (по правилу «180 дней»): 1" in text
    assert "🚫 В стоп-листе: 1" in text
    assert "🌍 Иностранные (отложены): 1" in text


def test_was_sent_today_same_content_detects_duplicates(tmp_path, monkeypatch):
    log_path = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log_path))
    monkeypatch.setattr(mu, "SENT_LOG_PATH", str(log_path))
    email = "user@example.com"
    subject = "Subject"
    body = "<p>Hello</p>"
    key = mu.canonical_for_history(email)
    content_hash = hashlib.sha1(f"{key}|{subject}|{body}".encode("utf-8")).hexdigest()
    ts = datetime.now(timezone.utc)
    messaging.log_sent_email(
        email,
        "group1",
        subject=subject,
        content_hash=content_hash,
        ts=ts,
    )
    assert mu.was_sent_today_same_content(email, subject, body)


def test_send_email_with_sessions_skips_duplicate_content(tmp_path, monkeypatch):
    html = tmp_path / "template.html"
    html.write_text("<html><body>Hi</body></html>", encoding="utf-8")
    log_path = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log_path))
    monkeypatch.setattr(mu, "SENT_LOG_PATH", str(log_path))
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.setattr(messaging, "EMAIL_PASSWORD", "secret")
    monkeypatch.setattr(messaging, "save_to_sent_folder", lambda *a, **k: None)
    monkeypatch.setattr(messaging, "cooldown_mark_sent", lambda *a, **k: None)
    monkeypatch.setattr(messaging, "should_skip_by_cooldown", lambda *_: (False, ""))

    class DummyClient:
        def send(self, from_addr, to_addr, raw):
            pass

    client = DummyClient()
    imap = object()

    outcome1, token1, log_key1, content_hash1 = messaging.send_email_with_sessions(
        client,
        imap,
        "Sent",
        "user@example.com",
        str(html),
        subject="Greeting",
    )
    assert outcome1 is messaging.SendOutcome.SENT
    assert token1
    assert log_key1
    assert content_hash1

    outcome2, token2, log_key2, content_hash2 = messaging.send_email_with_sessions(
        client,
        imap,
        "Sent",
        "user@example.com",
        str(html),
        subject="Greeting",
    )
    assert outcome2 is messaging.SendOutcome.DUPLICATE
    assert token2 == ""
    assert log_key2 is None
    assert content_hash2 is None

    with log_path.open(encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1


def test_build_message_adds_signature_and_unsubscribe(tmp_path, monkeypatch):
    html_file = tmp_path / "template.html"
    html_file.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    token_host = "example.com"
    monkeypatch.setenv("HOST", token_host)
    msg, token, _ = messaging.build_message(
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
    msg, _, _ = messaging.build_message(
        "r@example.com", str(html_file), "Subj"
    )
    html = msg.get_body("html").get_content()
    assert html.count("cid:logo") == 1
    assert html.count("<img") == 1

    monkeypatch.setenv("INLINE_LOGO", "0")
    msg2, _, _ = messaging.build_message(
        "r@example.com", str(html_file), "Subj"
    )
    html2 = msg2.get_body("html").get_content()
    assert "cid:logo" not in html2
    assert "<img" not in html2


def test_build_message_uses_explicit_group_metadata(tmp_path, monkeypatch):
    html_file = tmp_path / "template.html"
    html_file.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    msg, _, _ = messaging.build_message(
        "user@example.com",
        str(html_file),
        "Subject",
        group_title="Custom Label",
        group_key="custom-key",
    )

    assert msg["X-EBOT-Group"] == "Custom Label"
    assert msg["X-EBOT-Group-Key"] == "custom-key"
    assert msg["X-EBOT-Template-Label"] == "Custom Label"


def test_build_message_marks_override(tmp_path, monkeypatch):
    html_file = tmp_path / "template.html"
    html_file.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    msg, _, _ = messaging.build_message(
        "override@example.com",
        str(html_file),
        "Subject",
        override_180d=True,
    )
    assert msg["X-EBOT-Override-180d"] == "1"

    msg2, _, _ = messaging.build_message(
        "override@example.com",
        str(html_file),
        "Subject",
    )
    assert msg2.get("X-EBOT-Override-180d") is None


def test_repository_templates_logo_toggle(monkeypatch, tmp_path):
    template_path = tmp_path / "template.html"
    template_path.write_text(
        "<html><body><img src=\"cid:logo\" alt=\"logo\"/>{{BODY}}<br/><br/>{{SIGNATURE}}</body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    monkeypatch.setenv("INLINE_LOGO", "1")
    msg, _, _ = messaging.build_message("u@example.com", str(template_path), "Subj")
    html = msg.get_body("html").get_content()
    assert html.count("cid:logo") == 1
    assert html.count("<img") == 1

    monkeypatch.setenv("INLINE_LOGO", "0")
    msg2, _, _ = messaging.build_message("u@example.com", str(template_path), "Subj")
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


def test_limit_triggered_after_cap_sent(tmp_path, monkeypatch):
    log = tmp_path / "sent_log.csv"
    monkeypatch.setattr(messaging, "LOG_FILE", str(log))
    today = datetime.utcnow()
    with open(log, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["key", "email", "last_sent_at", "source", "status"])
        w.writeheader()
        for i in range(messaging.MAX_EMAILS_PER_DAY):
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
    assert imap.append_args[1] == "(\\Seen)"


def test_save_to_sent_folder_serializes_email_message():
    class DummyImap:
        def __init__(self):
            self.append_args = None

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
    assert imap.append_args[1] == "(\\Seen)"


def test_mark_unsubscribed_updates_log(temp_files):
    _, log_path = temp_files
    messaging.log_sent_email(
        "user@example.com", "group1", unsubscribe_token="tok123"
    )
    assert messaging.mark_unsubscribed("user@example.com")
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
    blocked_path, log_path = temp_files
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
        assert resp.status == 200
        assert "Ваш адрес больше не будет получать письма." in html
        resp_bad = await session.get(
            f"{base}/unsubscribe?email=user@example.com&token=bad"
        )
        assert resp_bad.status == 403
        resp_post = await session.post(
            f"{base}/unsubscribe",
            data={
                "List-Unsubscribe": "One-Click",
                "recipient": "post@example.com",
            },
        )
        assert resp_post.status == 200
        assert (await resp_post.text()) == "OK"
    await runner.cleanup()
    with open(log_path, encoding="utf-8") as f:
        row = next(csv.DictReader(f))
    assert row["unsubscribed"] == "1"
    assert "post@example.com" in blocked_path.read_text().splitlines()


def test_process_unsubscribe_requests_skips_without_imap_config(monkeypatch):
    monkeypatch.setenv("IMAP_HOST", "")
    monkeypatch.setenv("EMAIL_ADDRESS", "")
    monkeypatch.setenv("EMAIL_PASSWORD", "")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "")
    monkeypatch.setattr(messaging, "EMAIL_PASSWORD", "")

    called = {"imap": False}

    def fake_imap(*args, **kwargs):
        called["imap"] = True
        return object()

    monkeypatch.setattr(messaging, "imap_connect_ssl", fake_imap)
    messaging.process_unsubscribe_requests()
    assert called["imap"] is False


def test_process_unsubscribe_requests_uses_env_settings(monkeypatch):
    unsubscribed: list[str] = []
    monkeypatch.setenv("IMAP_HOST", "imap.example.com")
    monkeypatch.setenv("IMAP_PORT", "1143")
    monkeypatch.setenv("IMAP_TIMEOUT", "3.5")
    monkeypatch.setenv("EMAIL_ADDRESS", "env@example.com")
    monkeypatch.setenv("EMAIL_PASSWORD", "env-secret")
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "module@example.com")
    monkeypatch.setattr(messaging, "EMAIL_PASSWORD", "module-secret")
    monkeypatch.setattr(messaging, "mark_unsubscribed", lambda addr: unsubscribed.append(addr))

    calls: dict[str, object] = {}

    class DummyIMAP:
        def __init__(self):
            pass

        def login(self, user, password):
            calls["user"] = user
            calls["password"] = password

        def select(self, mailbox):
            calls["mailbox"] = mailbox
            return "OK", [b""]

        def search(self, charset, criteria):
            calls["search"] = (charset, criteria)
            return "OK", [b"1"]

        def fetch(self, num, spec):
            calls.setdefault("fetch", []).append((num, spec))
            raw = b"From: User <unsubscribe@example.com>\r\n\r\nBody"
            return "OK", [(b"1 (RFC822 {42}", raw)]

        def store(self, num, command, flags):
            calls.setdefault("store", []).append((num, command, flags))

        def logout(self):
            calls["logout"] = True

    def fake_connect(host, port, timeout=None):
        calls["host"] = host
        calls["port"] = port
        calls["timeout"] = timeout
        return DummyIMAP()

    monkeypatch.setattr(messaging, "imap_connect_ssl", fake_connect)
    messaging.process_unsubscribe_requests()

    assert unsubscribed == ["unsubscribe@example.com"]
    assert calls["host"] == "imap.example.com"
    assert calls["port"] == 1143
    assert calls["timeout"] == 3.5
    assert calls["user"] == "env@example.com"
    assert calls["password"] == "env-secret"
    assert calls["mailbox"] == "INBOX"
    assert calls["search"] == (None, '(UNSEEN SUBJECT "unsubscribe")')
    assert calls["fetch"] == [(b"1", "(RFC822)")]
    assert calls["store"] == [(b"1", "+FLAGS", "\\Seen")]
    assert calls.get("logout") is True


def test_send_email_idempotent(tmp_path, monkeypatch):
    html = tmp_path / "t.html"
    html.write_text("<html><body>Hi</body></html>", encoding="utf-8")
    sent: list[str] = []

    class DummySMTP:
        def __init__(self, *a, **kw):
            pass

        def send(self, *a, **kw):
            sent.append("1")

        def ensure(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(messaging, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(messaging, "save_to_sent_folder", lambda *a, **k: None)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "s@example.com")
    monkeypatch.setattr(messaging, "LOG_FILE", str(tmp_path / "log.csv"))
    monkeypatch.setattr(messaging, "_should_skip_by_history", lambda *_: (False, ""))

    first = messaging.send_email("u@example.com", str(html), batch_id="b1")
    second = messaging.send_email("u@example.com", str(html), batch_id="b1")
    assert len(sent) == 1
    assert first is messaging.SendOutcome.SENT
    assert second is messaging.SendOutcome.DUPLICATE


def test_send_email_respects_cooldown(tmp_path, monkeypatch):
    html = tmp_path / "t.html"
    html.write_text("<html><body>Hi</body></html>", encoding="utf-8")
    monkeypatch.setattr(messaging, "LOG_FILE", str(tmp_path / "log.csv"))
    monkeypatch.setattr(messaging, "_should_skip_by_history", lambda *_: (True, "cooldown"))

    called = {"send": 0}

    def fake_send(raw, recipient, max_tries=3):
        called["send"] += 1

    monkeypatch.setattr(messaging, "send_raw_smtp_with_retry", fake_send)

    result = messaging.send_email("user@example.com", str(html))

    assert result is messaging.SendOutcome.COOLDOWN
    assert called["send"] == 0


def test_domain_rate_limit(monkeypatch, caplog):
    times = [0, 0]

    def fake_monotonic():
        return times.pop(0)

    sleeps: list[float] = []

    def fake_sleep(sec):
        sleeps.append(sec)

    class DummySMTP:
        def __init__(self, *a, **kw):
            pass

        def send(self, *a, **kw):
            return None

        def ensure(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(messaging, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(messaging.time, "monotonic", fake_monotonic)
    monkeypatch.setattr(messaging.time, "sleep", fake_sleep)
    messaging._last_domain_send.clear()

    with caplog.at_level(logging.INFO):
        messaging.send_raw_smtp_with_retry("m", "a@example.com")
        messaging.send_raw_smtp_with_retry("m", "b@example.com")

    assert sleeps and sleeps[0] >= messaging._DOMAIN_RATE_LIMIT
    assert any("rate-limit" in r.message for r in caplog.records)


def test_soft_bounce_retry_and_no_suppress(monkeypatch, caplog):
    calls = {"n": 0}

    class DummySMTP:
        def __init__(self, *a, **kw):
            pass

        def send(self, *a, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise smtplib.SMTPResponseException(451, b"try again")

        def ensure(self):
            return None

        def close(self):
            return None

    sleeps: list[float] = []
    monkeypatch.setattr(messaging.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(messaging, "RobustSMTP", lambda *a, **k: DummySMTP())
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
    class DummySMTP:
        def __init__(self, *a, **kw):
            pass

        def send(self, *a, **kw):
            raise smtplib.SMTPResponseException(550, b"user unknown")

        def ensure(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(messaging, "RobustSMTP", lambda *a, **k: DummySMTP())
    monkeypatch.setattr(messaging, "_rate_limit_domain", lambda r: None)
    monkeypatch.setattr(messaging.time, "sleep", lambda s: None)

    called: list[str] = []
    monkeypatch.setattr(messaging, "suppress_add", lambda *a, **k: called.append("x"))
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "s@example.com")

    with pytest.raises(smtplib.SMTPResponseException):
        messaging.send_raw_smtp_with_retry("m", "a@example.com", max_tries=1)
    assert called == ["x"]
