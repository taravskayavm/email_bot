import pytest

from utils.email_clean import sanitize_email


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("user@mail.rurussia", "user@mail.ru"),
        ("user@test.comcom", "user@test.com"),
        ("user@site.runet", "user@site.ru"),
    ],
)
def test_domain_tail_repair(raw, expected, monkeypatch):
    monkeypatch.setenv("REPAIR_TLD_TAIL", "1")
    cleaned, reason = sanitize_email(raw)
    assert cleaned == expected, reason
