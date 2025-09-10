import re
from utils.email_clean import extract_emails, parse_emails_unified


def _one(s: str, expect: str):
    assert extract_emails(s) == [expect]
    assert parse_emails_unified(s) == [expect]


def test_dot_before_email():
    _one("Россия.duslem6704@mail.ru", "duslem6704@mail.ru")


def test_semicolon_before_email():
    _one("см.;ivan.petrov@gmail.com", "ivan.petrov@gmail.com")


def test_colon_before_email():
    _one("Контакт:sergey@mail.ru", "sergey@mail.ru")


def test_parentheses():
    _one("(oleg123@bk.ru)", "oleg123@bk.ru")
    _one("см.раздел-(alex@mail.ru)", "alex@mail.ru")


def test_quotes():
    _one('"maria@mail.ru"', "maria@mail.ru")
    _one("«anna@mail.ru»", "anna@mail.ru")
    _one("'dmitry@mail.ru'", "dmitry@mail.ru")


def test_dash_before_email():
    _one("см.раздел-ivan.petrov@gmail.com", "ivan.petrov@gmail.com")


def test_brackets():
    _one("[1]andrey@mail.ru", "andrey@mail.ru")
    _one("{2}pavel@mail.ru", "pavel@mail.ru")


def test_no_false_positive_inside_word():
    # не должны начинать матч внутри «слова»; адрес стартует на первой допустимой букве
    s = "ТекстРоссияduslem6704@mail.ru"
    assert extract_emails(s) == ["duslem6704@mail.ru"]
    assert parse_emails_unified(s) == ["duslem6704@mail.ru"]


def test_keeps_localpart_symbols():
    # допускаем . _ % + - внутри local-part — адрес остаётся «как в источнике»
    _one("см.: user.name+tag-test%ok@gmail.com", "user.name+tag-test%ok@gmail.com")


def test_with_invisibles_cleaned():
    # невидимые символы не мешают старту адреса
    s = "Россия.\u200b duslem6704@mail.ru"
    assert parse_emails_unified(s) == ["duslem6704@mail.ru"]

