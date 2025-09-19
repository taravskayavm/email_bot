import pytest

import config
import utils.email_clean as email_clean
from pipelines.extract_emails import run_pipeline_on_text


@pytest.fixture(autouse=True)
def enable_obfuscation(monkeypatch):
    monkeypatch.setattr(config, "OBFUSCATION_ENABLE", True, raising=False)
    monkeypatch.setattr(email_clean, "OBFUSCATION_ENABLE", True, raising=False)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("(a) anton-belousov0@rambler.ru", ["anton-belousov0@rambler.ru"]),
        ("... tsibulnikova2011@yandex.ru> 550 5.7.1 ...", ["tsibulnikova2011@yandex.ru"]),
        ("словоanton-belousov0@rambler.ru", ["anton-belousov0@rambler.ru"]),
        ("name(at)domain(dot)com", ["name@domain.com"]),
    ],
)
def test_trim_and_footnotes(raw, expected):
    final, dropped = run_pipeline_on_text(raw)
    assert sorted(final) == sorted(expected)


def test_role_prefix_from_bounce_is_filtered():
    raw = "RCPT TO:<russiavera.kidyaeva@yandex.ru>:"
    final, dropped = run_pipeline_on_text(raw)
    assert final == []
    assert ("russiavera.kidyaeva@yandex.ru", "role-like-prefix") in dropped
