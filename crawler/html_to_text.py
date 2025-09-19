try:
    from html2text import html2text
except Exception:  # pragma: no cover - safer fallback
    import re

    def html2text(html: str) -> str:  # type: ignore
        # Remove script/style blocks and basic tags when html2text is unavailable.
        s = re.sub(r"(?is)<(script|style)[^>]*>.*?</\\1>", " ", html or "")
        s = re.sub(r"(?i)<\s*(br|/p|/div|/li|/tr|/h[1-6])\s*>", "\n", s)
        s = re.sub(r"(?s)<[^>]+>", " ", s)
        s = re.sub(r"[\t \x0b\r\f]+", " ", s)
        s = re.sub(r"\n{2,}", "\n", s)
        return s.strip()

from utils.email_clean import _normalize_text


def html_to_text(html: str) -> str:
    """Convert HTML into normalized plain text."""
    text = html2text(html)
    return _normalize_text(text)
