import os
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
    value = msg["From"]
    # проверяем, что имя совпадает
    assert value.startswith(expected), f"unexpected From: {value}"
    # и адрес подтянулся из EMAIL_ADDRESS
    assert "<test@lanbook.ru>" in value

