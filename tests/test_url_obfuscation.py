from pathlib import Path

from emailbot.extraction import extract_from_url


def _write_html(tmp_path: Path, name: str, content: str) -> str:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return "file://" + str(p)


def test_requires_at_token(tmp_path):
    html = "<html><body>121536 gmail dot com</body></html>"
    url = _write_html(tmp_path, "70.html", html)
    hits, _ = extract_from_url(url)
    emails = [h.email for h in hits]
    assert "121536@gmail.com" not in emails


def test_obfuscation_with_at(tmp_path):
    html = "<html><body>121536 at gmail dot com</body></html>"
    url = _write_html(tmp_path, "71.html", html)
    hits, _ = extract_from_url(url)
    emails = [h.email for h in hits]
    assert "121536@gmail.com" in emails


def test_numeric_local_kept(tmp_path):
    html = "<html><body>2@mail.ru and 2 at mail dot ru</body></html>"
    url = _write_html(tmp_path, "72.html", html)
    hits, stats = extract_from_url(url)
    emails = [h.email for h in hits]
    assert "2@mail.ru" in emails
    assert stats["obfuscated_hits"] >= 1

