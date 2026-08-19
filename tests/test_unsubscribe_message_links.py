from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

from emailbot import messaging


def _template(tmp_path):
    path = tmp_path / "template.html"
    path.write_text("<html><body>Hello</body></html>", encoding="utf-8")
    return path


def test_unsubscribe_uses_mailto_when_public_url_is_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("UNSUBSCRIBE_PUBLIC_URL", raising=False)
    monkeypatch.delenv("HOST", raising=False)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    msg, _token = messaging.build_message(
        "recipient@example.com", str(_template(tmp_path)), "Subject"
    )

    html_body = msg.get_body("html").get_content()
    assert msg["Subject"] == "Subject"
    assert "mailto:sender@example.com?subject=" in html_body
    href = re.search(r'href="(mailto:[^"]+)"', html_body).group(1).replace(
        "&amp;", "&"
    )
    parsed = urlparse(href)
    query = parse_qs(parsed.query)
    assert query["subject"] == ["unsubscribe"]
    assert "Прошу отписать этот адрес от рассылки." in query["body"][0]
    assert "Адрес получателя: recipient@example.com" in query["body"][0]
    assert "mailto:sender@example.com?subject=" in msg["List-Unsubscribe"]
    assert msg.get("List-Unsubscribe-Post") is None
    assert "example.com/unsubscribe" not in html_body


def test_unsubscribe_uses_public_one_click_when_configured(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "UNSUBSCRIBE_PUBLIC_URL", "https://mail.example.org/unsubscribe"
    )
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    msg, token = messaging.build_message(
        "recipient@example.com", str(_template(tmp_path)), "Subject"
    )

    expected = (
        "https://mail.example.org/unsubscribe"
        f"?email=recipient%40example.com&token={token}"
    )
    assert msg["Subject"] == "Subject"
    assert expected.replace("&", "&amp;") in msg.get_body("html").get_content()
    assert expected in msg["List-Unsubscribe"]
    assert msg["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
