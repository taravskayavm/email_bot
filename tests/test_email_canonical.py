from utils.email_clean import canonicalize_email


def test_gmail_dots_and_plus():
    assert canonicalize_email("first.last+tag@gmail.com") == "firstlast@gmail.com"
    assert canonicalize_email("f.i.r.s.t.l.a.s.t@googlemail.com") == "firstlast@googlemail.com"


def test_mailru_plus_only():
    assert canonicalize_email("user+xx@yandex.ru") == "user@yandex.ru"
    assert canonicalize_email("user.name@mail.ru") == "user.name@mail.ru"
