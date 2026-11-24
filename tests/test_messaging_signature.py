import re
from emailbot import messaging


def test_logo_inline_and_signature_styles(tmp_path, monkeypatch):
    html = (
        '<html><head><style>'
        'body{font-family:"Times New Roman", serif;font-size:16px;}'
        '</style></head><body>'
        '<div style="text-align:right;"><img src="cid:logo" alt="l"></div>'
        '<p>Hello</p></body></html>'
    )
    html_file = tmp_path / "template.html"
    html_file.write_text(html, encoding="utf-8")
    logo = tmp_path / "Logo.png"
    logo.write_bytes(b"fake")
    monkeypatch.setattr(messaging, "SCRIPT_DIR", tmp_path)
    monkeypatch.setattr(messaging, "EMAIL_ADDRESS", "sender@example.com")

    monkeypatch.setenv("INLINE_LOGO", "1")
    msg, _ = messaging.build_message("r@example.com", str(html_file), "Subj")
    container = msg.get_payload()[-1]
    content = container.get_payload()[0].get_content()
    assert content.count("cid:logo") == 1
    assert content.count("<img") == 1
    payload = container.get_payload()
    assert len(payload) == 2
    assert payload[1]["Content-ID"] == "<logo>"

    sig_match = re.search(
        r'<div style="[^>]*font-family:([^;]+);[^>]*font-size:(\d+)px', content
    )
    assert sig_match
    assert "Times New Roman" in sig_match.group(1)
    assert int(sig_match.group(2)) == 15
    signature_block = re.search(
        r'<div style="[^>]*font-family:[^>]+>(.*?)</div>', content, re.DOTALL
    ).group(1)
    assert "cid:logo" not in signature_block and "<img" not in signature_block

    monkeypatch.setenv("INLINE_LOGO", "0")
    msg2, _ = messaging.build_message("r@example.com", str(html_file), "Subj")
    html_part2 = msg2.get_body("html")
    content2 = html_part2.get_content()
    assert "cid:logo" not in content2
    assert "<img" not in content2
    assert list(html_part2.iter_attachments()) == []
