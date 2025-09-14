from docx import Document

from utils.extraction_pdf import cleanup_text


def extract_text_from_docx(path: str) -> str:
    doc = Document(path)
    raw = "\n".join(p.text for p in doc.paragraphs)
    return cleanup_text(raw)

