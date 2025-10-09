import pytest

from emailbot.utils.email_clean import clean_and_normalize_email


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("user@example.com", "user@example.com"),
        ("User.Name+tag@Example.COM", "User.Name+tag@example.com"),
        ("user@пример.рф", "user@xn--e1afmkfd.xn--p1ai"),
        ("  <user@example.com>  ", "user@example.com"),
        ("\u200buser@example.com\u200b", "user@example.com"),
    ],
)
def test_valid_emails(raw: str, expected: str) -> None:
    email, reason = clean_and_normalize_email(raw)
    assert email == expected
    assert reason is None


@pytest.mark.parametrize(
    "raw,reason",
    [
        ("some.text.", "no_at_sign"),
        ("email at example dot com", "no_at_sign"),
        ("@example.com", "empty_local_or_domain"),
        ("user@", "empty_local_or_domain"),
        ("имя@пример.рф", "local_not_ascii"),
        (".user@example.com", "local_edge_dot"),
        ("user.@example.com", "local_edge_dot"),
        ("user..name@example.com", "local_consecutive_dots"),
        ("user()@example.com", "local_bad_chars"),
        ("user<>@example.com", "local_bad_chars"),
        ("user@-example.com", "domain_label_dash"),
        ("user@example-.com", "domain_label_dash"),
        ("user@example", "domain_bad_shape"),
        ("user@" + ("a" * 64) + ".com", "domain_label_size"),
    ],
)
def test_reject_emails(raw: str, reason: str) -> None:
    email, result_reason = clean_and_normalize_email(raw)
    assert email is None
    assert str(result_reason) == reason
