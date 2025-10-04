import re
from pathlib import Path


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


def _fitz_extract(path: Path) -> str:
    try:
        import fitz  # type: ignore
    except Exception:
        return ""

    doc = None
    try:
        doc = fitz.open(str(path))
    except Exception:
        return ""

    chunks: list[str] = []
    try:
        for page in doc:
            try:
                text = page.get_text() or ""
            except Exception:
                text = ""
            if text:
                chunks.append(text)
    finally:
        try:
            if doc is not None:
                doc.close()
        except Exception:
            pass
    return "\n".join(chunks)


def _extract_with_pypdf(path: Path) -> str:
    try:
        import pypdf
    except Exception:
        return ""

    try:
        reader = pypdf.PdfReader(str(path))
    except Exception:
        return ""

    chunks: list[str] = []
    for page in getattr(reader, "pages", []) or []:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            chunks.append(text)
    return "\n".join(chunks)


def _pdfminer_extract(path: Path) -> str:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
    except Exception:
        return ""

    try:
        return pdfminer_extract(str(path)) or ""
    except Exception:
        return ""


def extract_text_from_pdf(path: str | Path) -> str:
    pdf_path = Path(path)

    text = _fitz_extract(pdf_path)
    if not text or not text.strip():
        fallback = _extract_with_pypdf(pdf_path)
        text = fallback if fallback.strip() else ""
    if not text:
        text = _pdfminer_extract(pdf_path)
    if not text:
        return ""
    text = cleanup_text(text)
    return separate_around_emails(text)

