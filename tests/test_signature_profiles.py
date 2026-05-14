import json
from email.utils import parseaddr
from pathlib import Path

from emailbot import messaging


def test_labels_use_general_signature_only_for_requested_directions():
    labels = json.loads(Path("templates/_labels.json").read_text(encoding="utf-8"))
    general_groups = {"pedagogy", "sociology", "politology", "psychology", "geography"}

    for group, metadata in labels.items():
        expected_signature = "general" if group in general_groups else "old"
        assert metadata["signature"] == expected_signature


def test_general_signature_profile_updates_from_and_position(monkeypatch):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    template_info = messaging.get_template("pedagogy")
    msg, _token = messaging.build_message(
        "recipient@example.com",
        template_info["path"],
        "Subject",
        group_key="pedagogy",
        group_title=template_info["label"],
    )

    name, address = parseaddr(str(msg["From"]))
    html = msg.get_body("html").get_content()

    assert name == "Редакция литературы"
    assert address == "sender@example.com"
    assert "Заведующая редакцией литературы<br>" in html
    assert "Заведующая редакцией литературы по медицине, спорту и туризму" not in html


def test_old_signature_profile_keeps_from_and_position(monkeypatch):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    template_info = messaging.get_template("sport")
    msg, _token = messaging.build_message(
        "recipient@example.com",
        template_info["path"],
        "Subject",
        group_key="sport",
        group_title=template_info["label"],
    )

    name, address = parseaddr(str(msg["From"]))
    html = msg.get_body("html").get_content()

    assert name == "Редакция литературы по медицине, спорту и туризму"
    assert address == "sender@example.com"
    assert "Заведующая редакцией литературы по медицине, спорту и туризму" in html


def test_missing_or_unknown_signature_profile_falls_back_to_old(monkeypatch, tmp_path):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    html_file = tmp_path / "unknown.html"
    html_file.write_text("<html><body>{{SIGNATURE}}</body></html>", encoding="utf-8")

    for metadata in ({"signature": "unexpected"}, {}):
        def fake_get_template(code):
            return {
                "code": code,
                "label": "Unknown",
                "path": str(html_file),
                **metadata,
            }

        monkeypatch.setattr(messaging, "get_template", fake_get_template)

        msg, _token = messaging.build_message(
            "recipient@example.com",
            str(html_file),
            "Subject",
            group_key="unknown",
            group_title="Unknown",
        )

        name, address = parseaddr(str(msg["From"]))
        html = msg.get_body("html").get_content()

        assert name == "Редакция литературы по медицине, спорту и туризму"
        assert address == "sender@example.com"
        assert "Заведующая редакцией литературы по медицине, спорту и туризму" in html


def test_session_send_uses_general_signature_profile(monkeypatch, tmp_path):
    monkeypatch.setattr(messaging, "LOG_FILE", str(tmp_path / "sent_log.csv"))
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)
    monkeypatch.setattr(messaging.ledger, "record_send", lambda *a, **k: None)

    sent_messages = []

    class DummyClient:
        def send(self, msg):
            sent_messages.append(msg)

    template_info = messaging.get_template("politology")
    outcome, _token, _log_key, _content_hash = messaging.send_email_with_sessions(
        DummyClient(),
        object(),
        "Sent",
        "recipient-politology@example.com",
        template_info["path"],
        group_title=template_info["label"],
        group_key="politology",
        append_message=False,
    )

    assert outcome is messaging.SendOutcome.SENT
    msg = sent_messages[0]
    name, address = parseaddr(str(msg["From"]))
    html = msg.get_body("html").get_content()

    assert name == "Редакция литературы"
    assert address == "sender@example.com"
    assert "Заведующая редакцией литературы<br>" in html
    assert "Заведующая редакцией литературы по медицине, спорту и туризму" not in html


def test_session_send_keeps_old_signature_profile(monkeypatch, tmp_path):
    monkeypatch.setattr(messaging, "LOG_FILE", str(tmp_path / "sent_log.csv"))
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)
    monkeypatch.setattr(messaging.ledger, "record_send", lambda *a, **k: None)

    sent_messages = []

    class DummyClient:
        def send(self, msg):
            sent_messages.append(msg)

    template_info = messaging.get_template("highmedicine")
    outcome, _token, _log_key, _content_hash = messaging.send_email_with_sessions(
        DummyClient(),
        object(),
        "Sent",
        "recipient-highmedicine@example.com",
        template_info["path"],
        group_title=template_info["label"],
        group_key="highmedicine",
        append_message=False,
    )

    assert outcome is messaging.SendOutcome.SENT
    msg = sent_messages[0]
    name, address = parseaddr(str(msg["From"]))
    html = msg.get_body("html").get_content()

    assert name == "Редакция литературы по медицине, спорту и туризму"
    assert address == "sender@example.com"
    assert "Заведующая редакцией литературы по медицине, спорту и туризму" in html
