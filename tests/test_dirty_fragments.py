import pytest

from pipelines.extract_emails import run_pipeline_on_text


@pytest.mark.parametrize("raw,expected", [
    ("RCPT TO:<russiavera.kidyaeva@yandex.ru>:", ["russiavera.kidyaeva@yandex.ru"]),
    ("... tsibulnikova2011@yandex.ru> 550 5.7.1 ...", ["tsibulnikova2011@yandex.ru"]),
    ("(a) anton-belousov0@rambler.ru", ["anton-belousov0@rambler.ru"]),
])
def test_trim_and_footnotes(raw, expected):
    final, dropped = run_pipeline_on_text(raw)
    assert sorted(final) == sorted(expected)
