"""Utilities to extract e-mail addresses from uploaded files."""

from __future__ import annotations

import csv
import html
import io
import re
import zipfile
from typing import Dict, List, Tuple
from urllib.parse import unquote

from emailbot.utils.email_clean import clean_and_normalize_email

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}")
MAX_BYTES = 25 * 1024 * 1024  # 25 MB per file
ALLOWED_IN_ZIP = {".txt", ".csv", ".tsv", ".pdf", ".docx", ".xlsx", ".htm", ".html"}


class ExtractError(Exception):
    """Raised when we cannot process a file."""


def _norm_and_dedupe(cands: List[str]) -> Tuple[List[str], Dict[str, int]]:
    ok: List[str] = []
    seen: set[str] = set()
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


def _from_text(data: bytes) -> Tuple[List[str], Dict[str, int]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("latin-1", errors="ignore")
    return _norm_and_dedupe(EMAIL_RE.findall(text))


def _find_obfuscated_emails(text: str) -> List[str]:
    """Extract addresses that use simple textual obfuscation patterns."""

    local_part = r"(?P<local>[\w.+-]{1,64})"
    at_token = r"(?:\(|\[)?\s*(?:@|at|собака)\s*(?:\)|\])?"
    label = r"(?:[\w-]{1,63})"
    dot_token = r"(?:\(|\[)?\s*(?:\.|dot|точка)\s*(?:\)|\])?"
    pattern = re.compile(
        rf"\b{local_part}\b\s*{at_token}\s*\b{label}\b(?:\s*{dot_token}\s*\b{label}\b)*",
        re.IGNORECASE | re.UNICODE,
    )

    emails: list[str] = []
    seen: set[str] = set()
    connectors = {"at", "dot", "точка", "собака"}

    for match in pattern.finditer(text):
        local = match.group("local")
        tail = match.group(0)[match.end("local") - match.start(0) :]
        parts: list[str] = []
        for token in re.finditer(r"\b[\w-]{1,63}\b", tail, flags=re.UNICODE):
            value = token.group(0)
            if value.lower() in connectors:
                continue
            parts.append(value)
        if not parts:
            continue
        email = f"{local}@{'.'.join(parts)}"
        if email not in seen:
            seen.add(email)
            emails.append(email)
    return emails


def _from_html(data: bytes) -> Tuple[List[str], Dict[str, int]]:
    html_text = data.decode("utf-8", errors="ignore")
    mailto = re.findall(r"mailto:([^""' >]+)", html_text, flags=re.I)
    mailto = [unquote(m.split("?")[0]) for m in mailto]

    text = re.sub(r"<!--.*?-->", " ", html_text, flags=re.S)
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)

    cands = set(EMAIL_RE.findall(text))
    cands.update(_find_obfuscated_emails(text))
    cands.update(mailto)
    return _norm_and_dedupe(list(cands))


def _from_csv(data: bytes) -> Tuple[List[str], Dict[str, int]]:
    buf = io.StringIO(data.decode("utf-8", errors="ignore"))
    sample = buf.read(4096)
    buf.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(buf, dialect)
    cands: List[str] = []
    for row in reader:
        for cell in row:
            if cell:
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
                if val is not None:
                    cands.extend(EMAIL_RE.findall(str(val)))
    ok, rej = _norm_and_dedupe(cands)
    return ok, rej, None


def _from_docx(data: bytes) -> Tuple[List[str], Dict[str, int], str | None]:
    try:
        import docx  # type: ignore
    except Exception:
        return [], {"missing_dep_python_docx": 1}, "Для .docx нужен пакет python-docx"

    document = docx.Document(io.BytesIO(data))
    texts = [p.text for p in document.paragraphs]
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
    ok_all: List[str] = []
    rejects: Dict[str, int] = {}
    errors: List[str] = []

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            filename = info.filename
            if info.is_dir() or not _safe_zip_name(filename):
                if not _safe_zip_name(filename):
                    errors.append(f"{filename}: пропуск (путь)")
                continue
            if info.file_size > MAX_BYTES:
                errors.append(f"{filename}: пропуск (> {MAX_BYTES // (1024 * 1024)}MB)")
                continue
            ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            if ext not in ALLOWED_IN_ZIP:
                continue
            with zf.open(info) as fh:
                content = fh.read()
            ok, rej, warn = extract_emails_from_bytes(content, filename)
            ok_all.extend(ok)
            for key, val in rej.items():
                rejects[key] = rejects.get(key, 0) + val
            if warn:
                errors.append(f"{filename}: {warn}")

    ok_all = list(dict.fromkeys(ok_all))
    return ok_all, rejects, errors


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
