from emailbot.extraction import extract_obfuscated_hits


def test_obfuscation_strips_phone_prefix():
    text = "Контакты: +7 999 777 66 55 arsenii [собака] yandex [точка] ru"
    hits = extract_obfuscated_hits(text, source_ref="test")
    emails = {h.email for h in hits}
    assert "arsenii@yandex.ru" in emails or "arsenii.kamyshev@yandex.ru" in emails
    for email in emails:
        assert not email.startswith("+7"), email

