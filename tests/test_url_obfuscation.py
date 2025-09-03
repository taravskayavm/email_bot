from pathlib import Path

from emailbot.extraction import extract_from_url


def _write_html(tmp_path: Path, name: str, content: str) -> str:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return "file://" + str(p)


def test_numeric_obfuscation_dropped(tmp_path):
    html = (
        "<html><body>121536 [at] gmail [dot] com and 2 [at] mail [dot] ru" "</body></html>"
    )
    url = _write_html(tmp_path, "70.html", html)
    hits, stats = extract_from_url(url)
    emails = [h.email for h in hits]
    assert "121536@gmail.com" not in emails
    assert "2@mail.ru" not in emails
    assert stats["numeric_from_obfuscation_dropped"] >= 2


def test_numeric_mailto_and_direct_kept(tmp_path):
    html = (
        "<html><body><a href='mailto:12345@mail.ru'>m</a> 67890@mail.ru" "</body></html>"
    )
    url = _write_html(tmp_path, "71.html", html)
    hits, _ = extract_from_url(url)
    emails = [h.email for h in hits]
    assert "12345@mail.ru" in emails
    assert "67890@mail.ru" in emails

