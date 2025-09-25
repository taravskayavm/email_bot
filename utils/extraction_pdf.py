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


def _extract_with_pypdf(path: str) -> str:
    try:
        import pypdf
    except Exception:
        return ""

    try:
        reader = pypdf.PdfReader(path)
    except Exception:
        return ""

    chunks = []
    for page in getattr(reader, "pages", []) or []:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            chunks.append(text)
    return "\n".join(chunks)


def _extract_with_pdfminer(path: str) -> str:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
    except Exception:
        return ""

    try:
        return pdfminer_extract(path) or ""
    except Exception:
        return ""


def extract_text_from_pdf(path: str) -> str:
    raw = _extract_with_pypdf(path)
    if not raw:
        raw = _extract_with_pdfminer(path)
    if not raw:
        return ""
    raw = cleanup_text(raw)
    return separate_around_emails(raw)

