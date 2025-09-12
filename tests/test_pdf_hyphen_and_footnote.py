from utils.email_clean import parse_emails_unified


def test_does_not_eat_first_letter_after_footnote_marker():
    # без пробела и с пробелом — оба должны сохранить 'a' в начале
    assert parse_emails_unified("см. a)alex@mail.ru") == ["alex@mail.ru"]
    assert parse_emails_unified("см. a)  alex@mail.ru") == ["alex@mail.ru"]


def test_preserve_real_hyphen_inside_email_login():
    # из реального кейса: 'shestova-ma@inpsycho.ru'
    s = "Информация об авторе: shestova-ma@inpsycho.ru, https://..."
    assert parse_emails_unified(s) == ["shestova-ma@inpsycho.ru"]


def test_fix_soft_hyphen_linebreak_not_inside_email():
    # мягкий перенос вне адреса должен исчезнуть
    s = "Смотри табл.-\nцу 1, email: test@example.com"
    assert parse_emails_unified(s) == ["test@example.com"]
