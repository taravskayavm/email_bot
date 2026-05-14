import json
from email.utils import parseaddr
from pathlib import Path

from emailbot import messaging


GENERAL_SIGNATURE_DIRECTIONS = {
    "geography",
    "psychology",
    "pedagogy",
    "sociology",
    "politology",
}


def _message_html(msg):
    """Return HTML payload from a built email message."""

    html_part = msg.get_body("html")
    assert html_part is not None
    return html_part.get_content()


def test_labels_mark_only_general_signature_directions():
    """Only requested directions should use the general signature profile."""

    labels = json.loads(Path("templates/_labels.json").read_text(encoding="utf-8"))

    for code, meta in labels.items():
        expected_signature = (
            "general" if code in GENERAL_SIGNATURE_DIRECTIONS else "old"
        )
        assert meta["signature"] == expected_signature


def test_politology_uses_general_sender_and_signature(monkeypatch):
    """Politology should use the general From display name and position."""

    monkeypatch.setenv("EMAIL_ADDRESS", "test@lanbook.ru")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    msg, _token = messaging.build_message(
        "recipient@example.com",
        "templates/politology.html",
        "Subject",
    )

    name, addr = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert addr == "test@lanbook.ru"
    assert "Заведующая редакцией литературы" in _message_html(msg)
    assert "Заведующая редакцией литературы по медицине" not in _message_html(msg)


def test_highmedicine_keeps_old_sender_and_signature(monkeypatch):
    """Highmedicine should keep the old From display name and position."""

    monkeypatch.setenv("EMAIL_ADDRESS", "test@lanbook.ru")
    monkeypatch.delenv("EMAIL_FROM_NAME", raising=False)

    msg, _token = messaging.build_message(
        "recipient@example.com",
        "templates/highmedicine.html",
        "Subject",
    )

    name, addr = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы по медицине, спорту и туризму"
    assert addr == "test@lanbook.ru"
    assert (
        "Заведующая редакцией литературы по медицине, спорту и туризму"
        in _message_html(msg)
    )
