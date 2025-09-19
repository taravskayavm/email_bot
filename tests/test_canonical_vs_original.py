from utils.email_clean import canonicalize_email, dedupe_keep_original


def test_gmail_dots_and_plus_are_canonicalized():
    originals = [
        "y.pavelchuk.xhe@gmail.com",
        "ypavelchukxhe+tag@gmail.com",
        "Y.PAVELCHUK.XHE@GMAIL.COM",
    ]
    canon = [canonicalize_email(x) for x in originals]
    # все каноны должны совпасть
    assert len(set(canon)) == 1
    # дедуп сохранит первый оригинал (с точками)
    keep = dedupe_keep_original(originals)
    assert keep == ["y.pavelchuk.xhe@gmail.com"]


def test_yandex_plus_is_removed_in_canonical_only():
    originals = ["ivan.petrov+yad@yandex.ru", "ivan.petrov@yandex.ru"]
    keep = dedupe_keep_original(originals)
    # дубликат выкинут, но первый вариант (с +yad) сохранится для отправки
    assert keep == ["ivan.petrov+yad@yandex.ru"]


def test_other_domains_keep_dots():
    originals = ["name.surname@uni.com", "namesurname@uni.com"]
    # для других доменов точки значимы → это не дубли
    keep = dedupe_keep_original(originals)
    assert keep == originals
