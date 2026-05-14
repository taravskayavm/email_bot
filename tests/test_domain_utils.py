from emailbot.domain_utils import classify_email_domain, count_domains


def test_classify_email_domain_basic():
    assert classify_email_domain("user@gmail.com") == "global_mail"
    assert classify_email_domain("user@outlook.com") == "global_mail"
    assert classify_email_domain("user@mail.ru") == "global_mail"
    assert classify_email_domain("ivan@company.ru") == "ru_like"
    assert classify_email_domain("anna@univ.edu") == "foreign_corporate"
    assert classify_email_domain("rk@corp.com") == "foreign_corporate"
    assert classify_email_domain("bad@@example") == "unknown"


def test_count_domains():
    emails = [
        "a@gmail.com", "b@mail.ru", "c@outlook.com",
        "d@company.ru", "e@corp.com", "f@uni.de"
    ]
    stats = count_domains(emails)
    assert stats["global_mail"] == 3
    assert stats["ru_like"] == 1
    assert stats["foreign_corporate"] == 2
    assert stats["unknown"] == 0
