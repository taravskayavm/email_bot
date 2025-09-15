import json
import os
from pathlib import Path

import pytest

from utils.email_clean import sanitize_email, parse_emails_unified
from utils import send_stats


def test_leading_letters_and_digits_preserved():
    # обычные адреса не должны резаться
    assert sanitize_email("andrewvlasov@mail.ru") == "andrewvlasov@mail.ru"
    assert (
        sanitize_email("bogomolov.g.v@vniifk.ru")
        == "bogomolov.g.v@vniifk.ru"
    )
    assert sanitize_email("0-ju@mail.ru") == "0-ju@mail.ru"


def test_superscript_digits_are_stripped():
    # надстрочные символы убираются
    assert sanitize_email("\u00B9alex@mail.ru") == "alex@mail.ru"  # ¹alex
    assert sanitize_email("\u2075bob@mail.ru") == "bob@mail.ru"  # ⁵bob


def test_invisible_chars_are_removed():
    # soft hyphen в начале
    raw = "\xadandrewvlasov@mail.ru"
    assert sanitize_email(raw) == "andrewvlasov@mail.ru"
    # zero-width space
    raw2 = "\u200bbulykina.lv@yandex.ru"
    assert sanitize_email(raw2) == "bulykina.lv@yandex.ru"


def test_hyphenated_split_fixed():
    raw = "and-\nrewvlasov@mail.ru"
    assert sanitize_email(raw) == "andrewvlasov@mail.ru"


def test_send_stats_success_and_error(tmp_path, monkeypatch):
    # перенаправим send_stats.jsonl в tmp
    stats_path = tmp_path / "send_stats.jsonl"
    monkeypatch.setenv("SEND_STATS_PATH", str(stats_path))

    # Логируем успех
    send_stats.log_success("ok@example.com", "sport")
    # Логируем ошибку
    send_stats.log_error("fail@example.com", "sport", "550 user not found")
    # Логируем bounce
    send_stats.log_bounce("bounce@example.com", "user unknown", uuid="u1", message_id="m1")

    data = [json.loads(line) for line in stats_path.read_text().splitlines()]
    emails = {d["email"]: d for d in data}

    assert "ok@example.com" in emails
    assert emails["ok@example.com"]["status"] == "success"
    assert "fail@example.com" in emails
    assert emails["fail@example.com"]["status"] == "error"
    assert "bounce@example.com" in emails
    assert emails["bounce@example.com"]["status"] == "bounce"

    # Проверим агрегатор
    summary = send_stats.summarize_today()
    assert summary["success"] >= 1
    assert summary["error"] >= 2

