from utils.email_clean import parse_emails_unified


def _one(src, expect):
    assert parse_emails_unified(src) == [expect]


def test_colon_space_before_email():
    _one("e-mail: belyova.anka@gmail.com, ORCID: ...", "belyova.anka@gmail.com")


def test_after_comma_word_glued():
    _one("контакты,alexagorr@yandex.ru — отдел", "alexagorr@yandex.ru")


def test_dash_before_email():
    _one("— alex@mail.ru", "alex@mail.ru")
    _one("—alex@mail.ru", "alex@mail.ru")


def test_cyr_word_dot_before_email():
    _one("Россия.duslem6704@mail.ru", "duslem6704@mail.ru")


def test_letter_markers_a_b_c_not_removed_before_email():
    _one("см. a)alex@mail.ru", "alex@mail.ru")
    _one("см. b)boris@mail.ru", "boris@mail.ru")
    _one("см. c)carol@mail.ru", "carol@mail.ru")
