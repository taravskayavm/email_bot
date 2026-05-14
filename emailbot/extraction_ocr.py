import os
from pathlib import Path
from typing import Set


def ocr_emails_from_pdf(pdf_path: Path) -> Set[str]:
    if os.getenv("EMAILBOT_ENABLE_OCR", "0") != "1":
        return set()
    try:
        from pdf2image import convert_from_path
        import pytesseract
        from emailbot.parsing.email_patterns import extract_emails
    except Exception:
        return set()
    found: Set[str] = set()
    try:
        images = convert_from_path(str(pdf_path), dpi=200, fmt="png")
        for im in images[:20]:
            text = pytesseract.image_to_string(im)
            found |= extract_emails(text)
    except Exception:
        return set()
    return found
