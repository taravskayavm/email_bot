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
