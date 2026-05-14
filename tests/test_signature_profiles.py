from email.utils import parseaddr

from emailbot import messaging


def _write_template(tmp_path):
    """Create a minimal HTML template with the signature placeholder."""

    template_path = tmp_path / "template.html"
    template_path.write_text(
        "<html><body>{{SIGNATURE}}</body></html>",
        encoding="utf-8",
    )
    return template_path


def test_general_signature_profile_updates_from_and_position(monkeypatch, tmp_path):
    """General signature profile uses short From name and position line."""

    template_path = _write_template(tmp_path)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "test@lanbook.ru")
    monkeypatch.setattr(
        messaging,
        "get_template",
        lambda code: {"signature": "general"} if code == "general" else None,
    )

    msg, _token = messaging.build_message(
        "rcpt@example.com",
        str(template_path),
        "Subject",
        group_key="general",
    )

    name, addr = parseaddr(str(msg["From"]))
    html_body = msg.get_body("html").get_content()

    assert name == messaging.GENERAL_FROM_NAME
    assert addr == "test@lanbook.ru"
    assert messaging.GENERAL_SIGNATURE_POSITION in html_body
    assert messaging.DEFAULT_SIGNATURE_POSITION not in html_body


def test_old_signature_profile_keeps_default_from_and_position(monkeypatch, tmp_path):
    """Old signature profile preserves the legacy configurable signature."""

    template_path = _write_template(tmp_path)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "test@lanbook.ru")
    monkeypatch.setenv("EMAIL_FROM_NAME", "Custom Sender")
    monkeypatch.setattr(
        messaging,
        "get_template",
        lambda code: {"signature": "old"} if code == "old" else None,
    )

    msg, _token = messaging.build_message(
        "rcpt@example.com",
        str(template_path),
        "Subject",
        group_key="old",
    )

    name, addr = parseaddr(str(msg["From"]))
    html_body = msg.get_body("html").get_content()

    assert name == "Custom Sender"
    assert addr == "test@lanbook.ru"
    assert messaging.DEFAULT_SIGNATURE_POSITION in html_body
