from utils.email_clean import parse_emails_unified


def test_no_trim_single_letter_prefix_glued():
    s = "aivanov@mail.ru bpetrov@yandex.ru csmith@gmail.com"
    got = parse_emails_unified(s)
    assert "aivanov@mail.ru" in got
    assert "bpetrov@yandex.ru" in got
    assert "csmith@gmail.com" in got


def test_bullet_letter_not_eaten_if_glued():
    # даже если в тексте стояла «буквенная сноска» рядом, ядро не режем
    s = "a)ivanov@mail.ru; b)petrov@yandex.ru; c)smith@gmail.com"
    got = parse_emails_unified(s)
    assert "ivanov@mail.ru" in got
    assert "petrov@yandex.ru" in got
    assert "smith@gmail.com" in got


def test_numeric_footnote_removed_safely():
    s = "[12]ivanov@mail.ru (3)petrov@yandex.ru 5smith@gmail.com"
    got = parse_emails_unified(s)
    assert "ivanov@mail.ru" in got
    assert "petrov@yandex.ru" in got
    assert "smith@gmail.com" in got
