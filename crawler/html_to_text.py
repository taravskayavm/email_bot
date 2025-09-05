try:
    from html2text import html2text as _html2text
except Exception:  # pragma: no cover - fallback if html2text is missing
    def _html2text(html: str) -> str:  # type: ignore
        return html

from utils.email_clean import _normalize_text

def html_to_text(html: str) -> str:
    text = _html2text(html)
    return _normalize_text(text)
