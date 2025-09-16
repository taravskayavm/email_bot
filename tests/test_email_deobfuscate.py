from utils.email_deobfuscate import deobfuscate_text


def test_russian_markers():
    text = "ivan (собака) yandex (точка) ru"
    result = deobfuscate_text(text)
    assert result == "ivan@yandex.ru"
    assert set(deobfuscate_text.last_rules) == {"at", "dot"}


def test_bracketed_markers():
    text = "support [at] uni [dot] edu"
    result = deobfuscate_text(text)
    assert result == "support@uni.edu"


def test_word_without_context():
    text = "Напишите на собака"
    assert deobfuscate_text(text) == text


def test_inline_markers():
    text = "name(at)domain(dot)com"
    assert deobfuscate_text(text) == "name@domain.com"
