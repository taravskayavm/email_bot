"""Regression coverage for PDF link annotation boundaries."""

from emailbot.extraction import extract_emails_document
from emailbot.extraction_pdf import _collect_fitz_text, cleanup_text


class _FakePage:
    @staticmethod
    def get_text(*_args):
        return "Contact us\n"

    @staticmethod
    def get_links():
        return [
            {
                "uri": "mailto:hello@example.com?subject=Hello",
            }
        ]


def test_pdf_mailto_does_not_glue_visible_text_to_local_part():
    text, pages = _collect_fitz_text([_FakePage()])

    emails = extract_emails_document(cleanup_text(text), {})

    assert pages == 1
    assert emails == ["hello@example.com"]
    assert "us.hello@example.com" not in text
