from emailbot.extraction_url import extract_obfuscated_hits
from emailbot.extraction import EmailHit


def emails(hits):
    return sorted({h.email for h in hits})


def test_digits_and_one_letter_locals_are_dropped():
    html = """
      <p>статья 5 опубликована 2020.03, см. gmail.com</p>
      <p>a [at] yandex [dot] ru</p>
      <p>9 (собака) mail [точка] ru</p>
      <a href="mailto:info@example.com">mail us</a>
    """
    hits = extract_obfuscated_hits(html, "zip:/dummy.html")
    em = emails(hits)
    # мусор убран:
    assert "5@gmail.com" not in em
    assert "9@mail.ru" not in em
    assert "a@yandex.ru" not in em


def test_mailto_is_kept():
    html = '<a href="mailto:info@пример.рф?subject=x">mail us</a>'
    hits = extract_obfuscated_hits(html, "zip:/dummy.html")
    em = emails(hits)
    assert any(x.endswith("@xn--e1afmkfd.xn--p1ai") for x in em)
