def test_extract_email_with_footnote_and_line_breaks():
    """
    Извлечение адресов в сложных текстах:
      - Сноска рядом с e-mail не должна "съедать" часть адреса.
      - Разрыв строки внутри адреса должен корректно "склеиваться".
      - Не должно "приклеивать" соседние слова к почте.
    """
    from emailbot.extraction import smart_extract_emails

    # Сноска в квадратных скобках рядом с адресом + перенос в домене
    text = "Связь: ivanov.[1]@example.\ncom; резерв: a) petrov@example.org"
    hits = {e.lower() for e in smart_extract_emails(text)}

    assert "ivanov@example.com" in hits
    assert "petrov@example.org" in hits
    # Убеждаемся, что "a)" не склеилось с адресом
    assert "a)petrov@example.org" not in hits


def test_extract_email_with_at_break_and_hyphen_break():
    from emailbot.extraction import smart_extract_emails

    # Перенос после @ и перенос со знаком дефиса
    text = "name@\nexample.com\n\nsales-\nteam@company.ru"
    hits = {e.lower() for e in smart_extract_emails(text)}

    assert "name@example.com" in hits
    assert "salesteam@company.ru" in hits or "sales-team@company.ru" in hits
