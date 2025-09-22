from emailbot.bot_handlers import _classify_emails


def test_disjoint_sets():
    emails = [
        "ok@mail.ru",
        "suspect@mail.ru",
        "again@mail.ru",
        "x@qq.com",
    ]
    classes = _classify_emails(emails)
    S_all = classes["all"]
    S_sus = classes["sus"]
    S_foreign = classes["foreign"]
    S_cool = classes["cool"]
    S_send = classes["send"]

    assert not (S_sus & S_foreign)
    assert not (S_cool & S_foreign)
    assert not (S_send & (S_cool | S_sus | S_foreign))
    assert (S_send | S_cool | S_sus | S_foreign) <= S_all
