try:
    from html2text import html2text
except Exception:  # pragma: no cover - fallback if html2text is missing
    def html2text(html: str) -> str:  # type: ignore
        return html

from utils.email_clean import _normalize_text


def html_to_text(html: str) -> str:
    """Convert HTML into normalized plain text."""
    text = html2text(html)
    return _normalize_text(text)
