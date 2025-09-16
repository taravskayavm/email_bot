import pytest

import config
import utils.email_clean as email_clean
from pipelines.extract_emails import run_pipeline_on_text


@pytest.fixture(autouse=True)
def enable_obfuscation(monkeypatch):
    monkeypatch.setattr(config, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(email_clean, "OBFUSCATION_ENABLE", True, raising=False)


def test_obfuscated_russian_words():
    raw = "Иван (собака) yandex (точка) ru"
    final, _ = run_pipeline_on_text(raw)
    assert "Иван@yandex.ru".lower() in [e.lower() for e in final]


def test_obfuscated_english_words():
    raw = "support [at] uni [dot] edu"
    final, _ = run_pipeline_on_text(raw)
    assert "support@uni.edu" in final


def test_no_false_positive():
    raw = "Напишите слово собака в ответ"
    final, dropped = run_pipeline_on_text(raw)
    assert len(final) == 0
