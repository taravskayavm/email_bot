from emailbot.extraction import extract_emails_manual

def test_manual_includes_foreign_and_numeric():
    s = "User <user@mit.edu>, 12345@domain.ru (ok)"
    emails = sorted(set(map(str.lower, extract_emails_manual(s))))
    assert "user@mit.edu" in emails
    assert "12345@domain.ru" in emails
    # здесь мы проверяем именно парсер. Отправку без фильтров проверяйте в e2e,
    # либо мокните хэндлер: он НЕ должен отсекать эти адреса.
