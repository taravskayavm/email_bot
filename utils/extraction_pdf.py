from pdfminer.high_level import extract_text


INVISIBLES = ["\xad", "\u200b", "\u200c", "\u200d", "\ufeff"]


def cleanup_text(text: str) -> str:
    """Удаляем невидимые символы, которые часто «съедают» первую букву e-mail."""
    for ch in INVISIBLES:
        text = text.replace(ch, "")
    return text


def extract_text_from_pdf(path: str) -> str:
    raw = extract_text(path)
    return cleanup_text(raw)

