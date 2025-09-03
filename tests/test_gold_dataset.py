import pathlib

from emailbot.extraction import strip_html, smart_extract_emails


def test_html_fixture_numbers():
    html_path = pathlib.Path('tests/fixtures/html/footnotes.html')
    html = html_path.read_text(encoding='utf-8')
    text = strip_html(html)
    emails = smart_extract_emails(text)
    assert emails == ['john.doe@example.com', 'jane.doe@example.org']
