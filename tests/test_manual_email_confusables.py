from emailbot import messaging


def test_manual_email_parser_normalizes_cyrillic_lookalike():
    text = "natalia.kushnarenko1@gmail.com usсhakova.om@yandex.ru"

    assert messaging.parse_emails_from_text(text) == [
        "natalia.kushnarenko1@gmail.com",
        "uschakova.om@yandex.ru",
    ]
