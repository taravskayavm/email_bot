import pathlib

from emailbot.extraction import (
    strip_html,
    smart_extract_emails,
    extract_from_pdf,
    extract_from_url,
)
from tests.util_factories import make_pdf
from emailbot.extraction_common import filter_invalid_tld


def test_spa_and_sitemap(tmp_path, make_fetch):
    policy = make_pdf(tmp_path, [("office@site.ru license@site.ru", {})])
    spa_html = '<html><body>enable JavaScript<script src="app.js"></script></body></html>'
    sitemap = (
        "<?xml version='1.0'?><urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>http://test.local/docs/policy.pdf</loc></url></urlset>"
    )
    files = {
        "http://test.local/spa.html": spa_html,
        "http://test.local/app.js": "",
        "http://test.local/robots.txt": "Sitemap: http://test.local/sitemap.xml",
        "http://test.local/sitemap.xml": sitemap,
        "http://test.local/docs/policy.pdf": policy.read_bytes(),
    }
    fetch = make_fetch(files)
    hits, stats = extract_from_url("http://test.local/spa.html", fetch=fetch)
    emails = {h.email for h in hits}
    assert emails == {"office@site.ru", "license@site.ru"}
    assert stats.get("hits_sitemap", 0) > 0


def test_phone_prefix_stripped(tmp_path):
    html = (
        "<p>+7-913-123-45-67stark_velik@mail.ru valid@example.com "
        "01-37-93elena-ivanova@yandex.ru other@example.org</p>"
    )
    text = strip_html(html)
    stats: dict = {}
    emails = set(smart_extract_emails(text, stats))
    expected = {
        "stark_velik@mail.ru",
        "elena-ivanova@yandex.ru",
        "valid@example.com",
        "other@example.org",
    }
    assert expected <= emails
    assert stats.get("phone_prefix_stripped", 0) > 0


def test_tag_break_keeps_letter(tmp_path):
    html = "<p><span>b</span>iathlon@yandex.ru •biathlon@yandex.ru</p>"
    text = strip_html(html)
    emails = set(smart_extract_emails(text))
    assert emails == {"biathlon@yandex.ru"}


def test_pdf_footnotes(tmp_path):
    blocks = [
        ("¹", {"superscript": True}),
        ("96soul@mail.ru", {}),
    ]
    pdf = make_pdf(tmp_path, blocks)
    hits, stats = extract_from_pdf(str(pdf))
    emails = {h.email for h in hits}
    assert emails == {"96soul@mail.ru"}


def test_obfuscations(tmp_path, make_fetch):
    html = (
        "<p>user [at] site [dot] ru, user&#64;site.ru, user&commat;site.ru.</p>"
        "<script src='bundle.js'></script>"
    )
    js = 'const x = atob("dXNlckBzaXRlLnJ1");'
    files = {
        "http://test.local/obf.html": html,
        "http://test.local/bundle.js": js,
    }
    fetch = make_fetch(files)
    hits, stats = extract_from_url("http://test.local/obf.html", fetch=fetch)
    emails = {h.email for h in hits}
    assert emails == {"user@site.ru"}


def test_tld_filter_docx(tmp_path):
    emails = ["good@site.ru", "bad@site.zzz", "test@host.abs"]
    filtered, stats = filter_invalid_tld(emails)
    assert set(filtered) == {"good@site.ru"}
    assert stats.get("invalid_tld", 0) == 2
