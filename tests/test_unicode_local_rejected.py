from utils.email_clean import sanitize_email as _sanitize_email


def sanitize_email(value: str, strip_footnote: bool = True) -> str:
    return _sanitize_email(value, strip_footnote)[0]


def test_mixed_script_local_is_rejected():
    assert sanitize_email("eвгeньeвичavolkov1960@gmail.com") == ""
    assert sanitize_email("пoльзoвaтeлeйt.stepanenko@alpfederation.ru") == ""
    assert sanitize_email("буxгaлтepn.sukhorukova@alpfederation.ru") == ""
