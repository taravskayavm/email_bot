import pytest

from emailbot.extraction import smart_extract_emails


@pytest.mark.parametrize(
    "text, expected",
    [
        ("john\u200B.doe@example.com", ["john.doe@example.com"]),
        ("joh\u00ADn.doe@example.com", ["john.doe@example.com"]),
        ("tеst@exаmple.com", ["test@example.com"]),
        ("john\n.doe@example.com", ["john.doe@example.com"]),
    ],
)
def test_extraction_handles_obfuscations(text, expected):
    assert smart_extract_emails(text) == expected


@pytest.mark.parametrize(
    "nums, domain",
    [
        ("1", "example.com"),
        ("123", "mail.ru"),
    ],
)
def test_no_number_domain_glue(nums, domain):
    text = nums + domain
    assert smart_extract_emails(text) == []
