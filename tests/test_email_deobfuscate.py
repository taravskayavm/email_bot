import pytest

import config
import pipelines.extract_emails as pipeline
import utils.email_clean as email_clean
from pipelines.extract_emails import run_pipeline_on_text


@pytest.fixture(autouse=True)
def enable_obfuscation(monkeypatch):
    monkeypatch.setattr(config, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(email_clean, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(pipeline, "PERSONAL_ONLY", False, raising=False)


def test_obfuscated_russian_words():
    raw = "Иван (собака) yandex (точка) ru"
    final, _ = run_pipeline_on_text(raw)
    assert final == []


def test_obfuscated_english_words():
    raw = "support [at] uni [dot] com"
    final, _ = run_pipeline_on_text(raw)
    assert "support@uni.com" in final


def test_no_false_positive():
    raw = "Напишите слово собака в ответ"
    final, dropped = run_pipeline_on_text(raw)
    assert len(final) == 0
