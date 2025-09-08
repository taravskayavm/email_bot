from utils.email_clean import sanitize_email


def test_mixed_script_local_is_rejected():
    assert sanitize_email("eвгeньeвичavolkov1960@gmail.com") == ""
    assert sanitize_email("пoльзoвaтeлeйt.stepanenko@alpfederation.ru") == ""
    assert sanitize_email("буxгaлтepn.sukhorukova@alpfederation.ru") == ""
