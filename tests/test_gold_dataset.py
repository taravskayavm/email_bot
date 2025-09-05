from pathlib import Path

from emailbot.extraction import strip_html, smart_extract_emails, extract_from_url


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _emails_from(hits) -> set[str]:
    return {h.email.lower() for h in hits}


def test_spa_sitemap(tmp_path, httpx_file_server):
    spa = _write(
        tmp_path / "spa.html",
        "enable JavaScript<script src=\"app.js\"></script>",
    )
    app = _write(tmp_path / "app.js", "")
    sitemap = _write(
        tmp_path / "sitemap.xml",
        "<?xml version='1.0'?><urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>http://test.local/policy.html</loc></url></urlset>",
    )
    policy = _write(tmp_path / "policy.html", "office@site.ru license@site.ru")
    robots = _write(
        tmp_path / "robots.txt",
        "Sitemap: http://test.local/sitemap.xml",
    )
    httpx_file_server(
        {
            "http://test.local/spa.html": spa,
            "http://test.local/app.js": app,
            "http://test.local/robots.txt": robots,
            "http://test.local/sitemap.xml": sitemap,
            "http://test.local/policy.html": policy,
        }
    )
    hits, stats = extract_from_url("http://test.local/spa.html")
    emails = _emails_from(hits)
    assert emails == {"office@site.ru", "license@site.ru"}
    assert stats.get("hits_sitemap", 0) > 0


def test_phone_prefix_stripped():
    html = (
        "+7-913-331-52-25stark_velik@mail.ru.\n"
        "01-37-93-11elena-dzhioeva@yandex.ru\n"
        "normal: user1@site.ru, help@site.org"
    )
    text = strip_html(html)
    stats: dict = {}
    emails = {e.lower() for e in smart_extract_emails(text, stats)}
    assert {
        "stark_velik@mail.ru",
        "elena-dzhioeva@yandex.ru",
        "user1@site.ru",
        "help@site.org",
    } <= emails
    assert "+7-913-331-52-25stark_velik@mail.ru" not in emails
    assert "01-37-93elena-dzhioeva@yandex.ru" not in emails
    assert stats.get("phone_prefix_stripped", 0) > 0


def test_tag_break_first_letter():
    html = "<span>b</span>iathlon@yandex.ru\n&#8226;biathlon@yandex.ru"
    text = strip_html(html)
    emails = {e.lower() for e in smart_extract_emails(text)}
    assert emails == {"biathlon@yandex.ru"}


def test_obfuscations(tmp_path, httpx_file_server):
    page = _write(
        tmp_path / "obf.html",
        "user [at] site [dot] ru user &#64; site &#46; ru"
        "<script src=\"bundle.js\"></script>",
    )
    bundle = _write(tmp_path / "bundle.js", 'atob("dXNlckBzaXRlLnJ1")')
    httpx_file_server(
        {
            "http://test.local/obf.html": page,
            "http://test.local/bundle.js": bundle,
        }
    )
    hits, stats = extract_from_url("http://test.local/obf.html")
    emails = _emails_from(hits)
    assert "user@site.ru" in emails


def test_tld_validator():
    html = "local@site.ru tri@hlon.org a.d@a.message +m@h.abs"
    text = strip_html(html)
    stats: dict = {}
    emails = {e.lower() for e in smart_extract_emails(text, stats)}
    assert emails == {"local@site.ru", "tri@hlon.org"}
    assert stats.get("invalid_tld", 0) >= 0

