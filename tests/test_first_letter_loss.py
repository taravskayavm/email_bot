from utils.email_clean import parse_emails_unified


def _one(src, expect):
    got = parse_emails_unified(src)
    assert got == [expect], (src, got)


def test_email_after_colon_and_comma():
    _one("e-mail: belyova.anka@gmail.com, ORCID: 0000", "belyova.anka@gmail.com")


def test_email_after_cyr_word_dot():
    _one("Россия.duslem6704@mail.ru", "duslem6704@mail.ru")


def test_email_after_comma_glued():
    _one("контакты,alexagorr@yandex.ru — отдел", "alexagorr@yandex.ru")


def test_email_after_dash_and_emdash():
    _one("—alex@mail.ru", "alex@mail.ru")
    _one("-alex@mail.ru", "alex@mail.ru")


def test_footnote_letters_a_b_c_do_not_eat_first_letter():
    _one("см. a)alex@mail.ru", "alex@mail.ru")
    _one("см. b)boris@mail.ru", "boris@mail.ru")
    _one("см. c)carol@mail.ru", "carol@mail.ru")
