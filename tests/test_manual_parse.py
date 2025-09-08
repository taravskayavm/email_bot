from bot.handlers.manual_send import parse_manual_input


def test_manual_multiline_commas_semicolons():
    text = """pavelshabalin@mail.ru
shataev1@rambler.ru, aaksviaz@mail.ru
elena-dzhioeva@yandex.ru; mashkov47@mail.ru
stark_velik@mail.ru
ovalov@gmail.com
"""
    emails = parse_manual_input(text)
    assert "ovalov@gmail.com" in emails
    assert not any("mail.ruovalov" in e for e in emails)
    assert set(emails) >= {
        "pavelshabalin@mail.ru",
        "shataev1@rambler.ru",
        "aaksviaz@mail.ru",
        "elena-dzhioeva@yandex.ru",
        "mashkov47@mail.ru",
        "stark_velik@mail.ru",
        "ovalov@gmail.com",
    }

