from utils.email_clean import parse_emails_unified, strip_invisibles

CASES = [
    "\u200bbubnovskaia.ov@dvfu.ru",        # ZWSP
    "\u00adbubnovskaia.ov@dvfu.ru",        # soft hyphen
    "\u2060bubnovskaia.ov@dvfu.ru",        # word joiner
    "\uFEFFbubnovskaia.ov@dvfu.ru",        # BOM
    "\u200F\u202Abubnovskaia.ov@dvfu.ru",  # RLM + LRE
]


def test_strip_invisibles_keeps_first_letter():
    for src in CASES:
        assert strip_invisibles(src).startswith("bubnovskaia"), repr(src)


def test_parse_email_after_invisibles():
    for src in CASES:
        got = parse_emails_unified(src)
        assert got == ["bubnovskaia.ov@dvfu.ru"], repr(src)
