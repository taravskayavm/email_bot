from utils.email_clean import parse_emails_unified


def _emails_meta(src: str):
    emails, meta = parse_emails_unified(src, return_meta=True)
    return set(emails), set(meta.get("suspects") or [])


def test_long_alpha_run_marked_suspect():
    # длинная буквенная «простыня» без разделителей (склейка слов)
    emails, suspects = _emails_meta("russiaanalexan@mail.ru ok@ok.ru")
    assert "russiaanalexan@mail.ru" in suspects


def test_orcid_like_prefix_marked_suspect():
    emails, suspects = _emails_meta("//orcid.org/0000-0003-2673-01192stolbov210857@mail.ru")
    assert any(e.endswith("@mail.ru") for e in emails)
    assert any(e.endswith("@mail.ru") for e in suspects)


def test_glued_prev_letter_context_marked_suspect():
    # Буква слева без разделителя → «склейка» (универсально, без словаря)
    src = "Контакты:Россияbelyova@mail.ru"
    emails, suspects = _emails_meta(src)
    assert "belyova@mail.ru" in emails
    assert "belyova@mail.ru" in suspects


def test_long_digit_head_marked_suspect():
    emails, suspects = _emails_meta("79082412863@yandex.ru another@site.ru")
    assert "79082412863@yandex.ru" in emails
    assert "79082412863@yandex.ru" in suspects
