from emailbot.extraction import extract_emails_document


def test_ru_words_sobaka_tochka() -> None:
    text = "Контакты: ivanov собака gmail точка com"
    assert extract_emails_document(text) == ["ivanov@gmail.com"]


def test_en_words_at_dot_with_spaces() -> None:
    text = "Write to: jane.doe at uni dot ru"
    assert extract_emails_document(text) == ["jane.doe@uni.ru"]


def test_split_letters_and_dashes() -> None:
    text = "e - m a i l : a n n a - p e t r o v a (at) y a n d e x (dot) r u"
    out = extract_emails_document(text)
    assert "anna-petrova@yandex.ru" in out
