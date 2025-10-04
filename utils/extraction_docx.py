from pathlib import Path

from docx import Document

from utils.extraction_pdf import cleanup_text, separate_around_emails


def extract_text_from_docx(path: str | Path) -> str:
    try:
        doc = Document(path)
    except Exception:
        return ""

    try:
        raw = "\n".join(p.text for p in doc.paragraphs)
    except Exception:
        return ""

    raw = cleanup_text(raw)
    return separate_around_emails(raw)

