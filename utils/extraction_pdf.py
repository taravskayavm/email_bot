from pdfminer.high_level import extract_text
import re


INVISIBLES = ["\xad", "\u200b", "\u200c", "\u200d", "\ufeff"]
SUPERSCRIPTS = "\u00B9\u00B2\u00B3" + "".join(chr(c) for c in range(0x2070, 0x207A))


def cleanup_text(text: str) -> str:
    """Удаляем невидимые символы, которые часто «съедают» первую букву e-mail."""
    for ch in INVISIBLES:
        text = text.replace(ch, "")
    text = text.translate({ord(c): None for c in SUPERSCRIPTS})
    text = re.sub(r"([A-Za-z0-9])-\n([A-Za-z0-9])", r"\1\2", text)
    return text


BASIC_EMAIL = r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"


def separate_around_emails(text: str) -> str:
    text = re.sub(rf"([^\s])({BASIC_EMAIL})", r"\1 \2", text)
    text = re.sub(rf"({BASIC_EMAIL})([^\s])", r"\1 \2", text)
    return text


def extract_text_from_pdf(path: str) -> str:
    raw = extract_text(path)
    raw = cleanup_text(raw)
    return separate_around_emails(raw)

