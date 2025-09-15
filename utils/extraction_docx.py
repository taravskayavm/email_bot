from docx import Document

from utils.extraction_pdf import cleanup_text, separate_around_emails


def extract_text_from_docx(path: str) -> str:
    doc = Document(path)
    raw = "\n".join(p.text for p in doc.paragraphs)
    raw = cleanup_text(raw)
    return separate_around_emails(raw)

