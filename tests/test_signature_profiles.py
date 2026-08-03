import json
from email.utils import parseaddr
from pathlib import Path

from emailbot import messaging


def _html_body(message):
    """Return the rendered HTML body from an EmailMessage."""

    html_part = message.get_body("html")
    assert html_part is not None
    return html_part.get_content()


def test_labels_use_general_signature_only_for_requested_directions():
    labels = json.loads(Path("templates/_labels.json").read_text(encoding="utf-8"))
    general_groups = {"geography", "psychology", "pedagogy", "sociology", "politology"}

    for group, metadata in labels.items():
        expected = "general" if group in general_groups else "old"
        assert metadata["signature"] == expected


def test_build_message_uses_general_profile_for_politology(monkeypatch):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    msg, _token = messaging.build_message(
        "recipient@example.com",
        "templates/politology.html",
        "Subject",
        group_key="politology",
    )

    name, address = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert address == "sender@example.com"
    assert "Заведующая редакцией литературы<br>" in _html_body(msg)
    assert (
        "Заведующая редакцией литературы по медицине, спорту и туризму<br>"
        not in _html_body(msg)
    )


def test_build_message_uses_old_profile_for_highmedicine(monkeypatch):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    msg, _token = messaging.build_message(
        "recipient@example.com",
        "templates/highmedicine.html",
        "Subject",
        group_key="highmedicine",
    )

    name, address = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы по медицине, спорту и туризму"
    assert address == "sender@example.com"
    assert (
        "Заведующая редакцией литературы по медицине, спорту и туризму<br>"
        in _html_body(msg)
    )


def test_unknown_or_missing_signature_falls_back_to_old(monkeypatch):
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    assert messaging._signature_profile_for_group(None) == "old"
    assert messaging._signature_profile_for_group("unknown-direction") == "old"

    def fake_get_template(_group_key):
        return {"signature": "unexpected"}

    with monkeypatch.context() as patched_context:
        patched_context.setattr(messaging, "get_template", fake_get_template)
        assert messaging._signature_profile_for_group("custom") == "old"

    msg, _token = messaging.build_message(
        "recipient@example.com",
        "templates/politology.html",
        "Subject",
    )

    name, address = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert address == "sender@example.com"


def test_legacy_new_profile_alias_is_used_consistently(monkeypatch, tmp_path):
    monkeypatch.setenv("EMAIL_ADDRESS", "sender@example.com")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)
    template_path = tmp_path / "legacy.html"
    template_path.write_text("<html><body>{{SIGNATURE}}</body></html>", encoding="utf-8")

    monkeypatch.setattr(
        messaging,
        "get_template",
        lambda _group: {
            "code": "legacy",
            "path": str(template_path),
            "signature": "new",
        },
    )

    msg, _token = messaging.build_message(
        "recipient@example.com",
        str(template_path),
        "Subject",
        group_key="legacy",
    )

    name, _address = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert "Заведующая редакцией литературы<br>" in _html_body(msg)
    assert "по медицине, спорту и туризму<br>" not in _html_body(msg)
