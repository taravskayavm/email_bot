"""Utilities to extract e-mail addresses from uploaded files."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Dict, Iterable, List, Set, Tuple

from emailbot.utils.email_clean import clean_and_normalize_email

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")
MAX_BYTES = 25 * 1024 * 1024  # 25 MB per file
ALLOWED_IN_ZIP = {".txt", ".csv", ".tsv", ".pdf", ".docx", ".xlsx", ".htm", ".html"}


class ExtractError(Exception):
    """Raised when we cannot process a file."""


def _norm_and_dedupe(cands: Iterable[str]) -> Tuple[List[str], Dict[str, int]]:
    ok: List[str] = []
    seen: Set[str] = set()
    rejects: Dict[str, int] = {}
    for raw in cands:
        email, reason = clean_and_normalize_email(raw)
        if email is None:
            key = str(reason) if reason else "unknown"
            rejects[key] = rejects.get(key, 0) + 1
            continue
        if email in seen:
            continue
        seen.add(email)
        ok.append(email)
    return ok, rejects


def _from_text(data: bytes, encoding_guess: str | None = None) -> Tuple[List[str], Dict[str, int]]:
    text: str
    if encoding_guess:
        text = data.decode(encoding_guess, errors="ignore")
    else:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("latin-1", errors="ignore")
    return _norm_and_dedupe(EMAIL_RE.findall(text))


def _from_html(data: bytes) -> Tuple[List[str], Dict[str, int]]:
    text = data.decode("utf-8", errors="ignore")
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return _norm_and_dedupe(EMAIL_RE.findall(text))


def _from_csv(data: bytes) -> Tuple[List[str], Dict[str, int]]:
    buf = io.StringIO(data.decode("utf-8", errors="ignore"))
    sample = buf.read(4096)
    buf.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    buf.seek(0)
    reader = csv.reader(buf, dialect)
    cands: List[str] = []
    for row in reader:
        for cell in row:
            if not cell:
                continue
            cands.extend(EMAIL_RE.findall(cell))
    return _norm_and_dedupe(cands)


def _from_xlsx(data: bytes) -> Tuple[List[str], Dict[str, int], str | None]:
    try:
        import openpyxl  # type: ignore
    except Exception:
        return [], {"missing_dep_openpyxl": 1}, "Для .xlsx нужен пакет openpyxl"

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    cands: List[str] = []
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            for val in row:
                if val is None:
                    continue
                cands.extend(EMAIL_RE.findall(str(val)))
    ok, rej = _norm_and_dedupe(cands)
    return ok, rej, None


def _from_docx(data: bytes) -> Tuple[List[str], Dict[str, int], str | None]:
    try:
        import docx  # type: ignore
    except Exception:
        return [], {"missing_dep_python_docx": 1}, "Для .docx нужен пакет python-docx"

    document = docx.Document(io.BytesIO(data))
    texts: List[str] = []
    for p in document.paragraphs:
        texts.append(p.text)
    for table in document.tables:
        for row in table.rows:
            for cell in row.cells:
                texts.append(cell.text)
    cands: List[str] = []
    for text in texts:
        cands.extend(EMAIL_RE.findall(text))
    ok, rej = _norm_and_dedupe(cands)
    return ok, rej, None


def _from_pdf(data: bytes) -> Tuple[List[str], Dict[str, int], str | None]:
    try:
        from pdfminer.high_level import extract_text
    except Exception:
        return [], {"missing_dep_pdfminer": 1}, "Для .pdf нужен пакет pdfminer.six"

    text = extract_text(io.BytesIO(data), maxpages=200) or ""
    ok, rej = _norm_and_dedupe(EMAIL_RE.findall(text))
    return ok, rej, None


def _safe_zip_name(name: str) -> bool:
    return not (name.startswith("/") or ".." in name.replace("\\", "/"))


def _from_zip(data: bytes) -> Tuple[List[str], Dict[str, int], List[str]]:
    errors: List[str] = []
    ok_all: List[str] = []
    rejects_total: Dict[str, int] = {}

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            fname = info.filename
            if not _safe_zip_name(fname):
                errors.append(f"{fname}: пропущен (подозрительный путь)")
                continue
            if info.is_dir():
                continue
            if info.file_size > MAX_BYTES:
                errors.append(
                    f"{fname}: пропущен (> {MAX_BYTES // (1024 * 1024)}MB)"
                )
                continue
            ext = "." + fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
            if ext not in ALLOWED_IN_ZIP:
                continue
            with zf.open(info) as fh:
                content = fh.read()
            ok, rejects, warn = extract_emails_from_bytes(content, fname)
            ok_all.extend(ok)
            for key, val in rejects.items():
                rejects_total[key] = rejects_total.get(key, 0) + val
            if warn:
                errors.append(f"{fname}: {warn}")

    ok_all = list(dict.fromkeys(ok_all))
    return ok_all, rejects_total, errors


def extract_emails_from_bytes(data: bytes, filename: str) -> Tuple[List[str], Dict[str, int], str | None]:
    """Extract addresses from ``data`` according to the file name."""

    if len(data) > MAX_BYTES:
        raise ExtractError("file_too_large")

    name = filename.lower()
    if name.endswith((".csv", ".tsv")):
        ok, rejects = _from_csv(data)
        return ok, rejects, None
    if name.endswith(".txt"):
        ok, rejects = _from_text(data)
        return ok, rejects, None
    if name.endswith((".html", ".htm")):
        ok, rejects = _from_html(data)
        return ok, rejects, None
    if name.endswith(".xlsx"):
        return _from_xlsx(data)
    if name.endswith(".docx"):
        return _from_docx(data)
    if name.endswith(".pdf"):
        return _from_pdf(data)
    if name.endswith(".zip"):
        ok_all, rejects, errors = _from_zip(data)
        warn = "; ".join(errors) if errors else None
        return ok_all, rejects, warn

    ok, rejects = _from_text(data)
    return ok, rejects, None
