import os
from email.message import EmailMessage
from email.utils import parseaddr

import pytest

from emailbot import messaging


@pytest.mark.parametrize(
    "group, expected",
    [
        ("medicine", "Редакция литературы по медицине, спорту и туризму"),
        ("sport", "Редакция литературы по медицине, спорту и туризму"),
        ("tourism", "Редакция литературы по медицине, спорту и туризму"),
        ("psychology", "Редакция литературы"),
        ("geography", "Редакция литературы"),
        ("bioinformatics", "Редакция литературы"),
    ],
)
def test_choose_from_header(monkeypatch, group, expected, tmp_path):
    monkeypatch.setenv("EMAIL_ADDRESS", "test@lanbook.ru")

    tpl_path = tmp_path / f"{group}.html"
    tpl_path.write_text("<html><body>{{SIGNATURE}}</body></html>", encoding="utf-8")

    def fake_get_template(code):
        if code == group:
            signature = "new" if code in {"psychology", "geography", "bioinformatics"} else "old"
            return {
                "code": code,
                "label": code.title(),
                "path": str(tpl_path),
                "signature": signature,
            }
        return None

    monkeypatch.setattr(messaging, "get_template", fake_get_template)

    msgs = messaging.build_messages_for_group(group, ["rcpt@example.com"], {})
    assert len(msgs) == 1
    msg = msgs[0]
    assert "From" in msg
    value = str(msg["From"])
    # проверяем, что имя и адрес совпадают
    name, addr = parseaddr(value)
    assert name == expected, f"unexpected From: {value}"
    assert addr == "test@lanbook.ru"


def test_apply_from_trims(monkeypatch):
    monkeypatch.setenv("EMAIL_ADDRESS", "test@lanbook.ru")

    # simulate custom from name with trailing dot, NBSP and space
    monkeypatch.setattr(
        messaging,
        "_choose_from_header",
        lambda group: "Редакция литературы.\u00a0 ",
    )

    msg = EmailMessage()
    msg["From"] = "Whatever <old@example.com>"
    messaging._apply_from(msg, "psychology")

    name, addr = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert addr == "test@lanbook.ru"

