import os
from email.message import EmailMessage
from email.utils import parseaddr

import pytest

from emailbot import messaging


@pytest.mark.parametrize(
    "group, expected",
    [
        ("медицина", "Редакция литературы по медицине, спорту и туризму"),
        ("спорт", "Редакция литературы по медицине, спорту и туризму"),
        ("туризм", "Редакция литературы по медицине, спорту и туризму"),
        ("психология", "Редакция литературы"),
        ("география", "Редакция литературы"),
        ("биоинформатика", "Редакция литературы"),
    ],
)
def test_choose_from_header(monkeypatch, group, expected):
    monkeypatch.setenv("EMAIL_ADDRESS", "test@lanbook.ru")
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
    messaging._apply_from(msg, "психология")

    name, addr = parseaddr(str(msg["From"]))
    assert name == "Редакция литературы"
    assert addr == "test@lanbook.ru"

