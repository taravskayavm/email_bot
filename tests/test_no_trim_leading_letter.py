from utils.email_clean import parse_emails_unified


def test_no_trim_leading_letter():
    s = "aivanov@mail.ru; bpetrov@yandex.ru; csmith@gmail.com"
    got = parse_emails_unified(s)
    assert "aivanov@mail.ru" in got
    assert "bpetrov@yandex.ru" in got
    assert any(x.startswith("csmith@") for x in got)
