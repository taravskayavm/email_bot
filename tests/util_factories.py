from __future__ import annotations

from pathlib import Path
import csv
import zipfile

import fitz  # PyMuPDF
from docx import Document
from openpyxl import Workbook


def make_pdf(tmp_path: Path, blocks: list[tuple[str, dict]]) -> Path:
    """Create a PDF file with ``blocks`` written sequentially.

    Each block is a ``(text, options)`` tuple.  Supported options:

    ``{"superscript": True}`` – render text in a smaller font shifted up.
    ``{"newline": True}`` – move cursor to the next line before rendering.
    """

    doc = fitz.open()
    page = doc.new_page()
    x, y = 72, 72
    fontsize = 12
    line_height = fontsize + 2
    for text, opts in blocks:
        sup = opts.get("superscript")
        parts = text.split("\n")
        for idx, part in enumerate(parts):
            if sup:
                size = fontsize * 0.7
                page.insert_text((x, y - size * 0.3), part, fontsize=size)
                width = size * 0.6 * len(part)
            else:
                page.insert_text((x, y), part, fontsize=fontsize)
                width = fontsize * 0.6 * len(part)
            x += width
            if idx < len(parts) - 1:
                y += line_height
                x = 72
        if opts.get("newline"):
            y += line_height
            x = 72
    path = tmp_path / "temp.pdf"
    doc.save(path)
    doc.close()
    return path


def make_docx(tmp_path: Path, lines: list[str]) -> Path:
    doc = Document()
    for line in lines:
        doc.add_paragraph(line)
    path = tmp_path / "temp.docx"
    doc.save(path)
    return path


def make_xlsx(tmp_path: Path, rows: list[list[str]]) -> Path:
    wb = Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    path = tmp_path / "temp.xlsx"
    wb.save(path)
    return path


def make_csv(tmp_path: Path, lines: list[str]) -> Path:
    path = tmp_path / "temp.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for line in lines:
            writer.writerow([line])
    return path


def make_zip(tmp_path: Path, paths: list[Path]) -> Path:
    path = tmp_path / "temp.zip"
    with zipfile.ZipFile(path, "w") as z:
        for p in paths:
            z.write(p, arcname=p.name)
    return path
