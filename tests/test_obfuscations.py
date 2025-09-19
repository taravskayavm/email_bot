from utils.email_clean import parse_emails_unified, dedupe_with_variants

CASES = [
    ("name[at]domain[dot]com", {"name@domain.com"}),
    ("name(at)university(dot)com", {"name@university.com"}),
    ("mailto:name.surname@domain.com", {"name.surname@domain.com"}),
    ("name\u2022surname@do\u00b7main.com", {"name.surname@domain.com"}),
    ("name@do-\nmain.com", {"name@domain.com"}),
    ("Иванов I. (i.ivanov@uni.ru)", {"i.ivanov@uni.ru"}),
    (
        "no-reply@journal.com; editor@journal.com; ivan.ivanov@uni.ru",
        {"no-reply@journal.com", "editor@journal.com", "ivan.ivanov@uni.ru"},
    ),
]


def test_obfuscations_and_dedupe():
    for raw, expected in CASES:
        cleaned, meta = parse_emails_unified(raw, return_meta=True)
        uniq = set(dedupe_with_variants(cleaned))
        assert expected.issubset(uniq)
