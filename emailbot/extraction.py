"""Email extraction and processing helpers."""

from __future__ import annotations

import os
import re
import zipfile
import random
import concurrent.futures
import tempfile
from pathlib import Path
from typing import Tuple, Set, List, Dict

import aiohttp
import fitz  # PyMuPDF
import pandas as pd
from docx import Document

import html as htmllib

from .utils import log_error


ALLOWED_TLDS = {"ru", "com"}
ALLOWED_TLD_PATTERN = "|".join(ALLOWED_TLDS)

# Precompiled regex patterns for heavy use
_RX_PROTECT = re.compile(
    r"(?im)\b([A-Za-z0-9])\s*[\-\)\]\u2010\u2011\u2012\u2013\u2014]\s*\n\s*(?=[A-Za-z][A-Za-z0-9._%+-]*@)"
)
_RX_DEHYPHEN = re.compile(
    r"([A-Za-z0-9._%+\-])[\-\u2010\u2011\u2012\u2013\u2014]\s*\n\s*([A-Za-z0-9._%+\-])"
)
_RX_JOIN_NOHYPHEN = re.compile(
    r"([A-Za-z]{3,})\s*\n\s*([A-Za-z][A-Za-z0-9._%+\-]*)@"
)
_RX_JOIN_DOT = re.compile(
    r"([A-Za-z]{2,})([._])\s*\n\s*([A-Za-z][A-Za-z0-9._%+\-]*)@"
)
_RX_JOIN_NUM = re.compile(r"([A-Za-z]{2,})\s*\n\s*([0-9]{1,6})\s*@")
_RX_CRLF = re.compile(r"[\r\n]+")
_RX_AT = re.compile(r"\s*@\s*")
_RX_DOT = re.compile(r"(@[A-Za-z0-9.-]+)\s*\.\s*([A-Za-z]{2,10})\b")
_RX_DOT_COM = re.compile(r"\.\s*c\s*o\s*m\b", re.I)
_RX_DOT_RU = re.compile(r"\.\s*r\s*u\b", re.I)
_PROV = r"(gmail|yahoo|hotmail|outlook|protonmail|icloud|aol|live|msn|mail|yandex|rambler|bk|list|inbox|ya)"
_RX_PROV1 = re.compile(rf"(@{_PROV}\.co)(?=[^\w]|$)", re.I)
_RX_PROV2 = re.compile(rf"(@{_PROV}\.co)\s*m\b", re.I)
_RX_SUFFIX = re.compile(r"(\.(?:ru|com))(?=[A-Za-z0-9])")

_PAT_A = re.compile(
    r"(?im)\b([a-z])\s*\n\s*([a-z][a-z0-9._%+\-]{2,})@([a-z0-9.-]+\.(?:ru|com))"
)
_PAT_B = re.compile(
    r"(?im)\b([a-z]{2,})\s*\n\s*([0-9]{1,6})\s*@([a-z0-9.-]+\.(?:ru|com))"
)

_PDF_CACHE = Path(tempfile.gettempdir()) / "pdf_cache"
_PDF_CACHE.mkdir(exist_ok=True)


def normalize_email(s: str) -> str:
    return (s or "").strip().lower()


def is_allowed_tld(email_addr: str) -> bool:
    e = normalize_email(email_addr)
    pattern = rf"@[A-Za-z0-9.-]+\.(?:{ALLOWED_TLD_PATTERN})$"
    return bool(re.search(pattern, e))


def strip_html(html: str) -> str:
    if not html:
        return ""
    s = html
    s = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</p\s*>", "\n", s)
    s = re.sub(r"(?i)</div\s*>", "\n", s)
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = htmllib.unescape(s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n\s*\n+", "\n", s)
    return s.strip()


def sample_preview(items, k: int) -> list[str]:
    lst = list(dict.fromkeys(items))
    if len(lst) <= k:
        return lst
    return random.sample(lst, k)


def remove_invisibles(text: str) -> str:
    """Remove zero-width and similar invisible characters.

    Currently strips soft hyphens (\u00ad), non-breaking hyphens (\u2011),
    zero-width spaces (\u200b) and converts non-breaking spaces (\xa0) to
    regular spaces.
    """
    if not text:
        return ""
    s = text
    s = s.replace("\u00ad", "").replace("\u2011", "").replace("\u200b", "")
    s = s.replace("\xa0", " ")
    return s


# ---------------- Предобработка текста ----------------
def _preclean_text_for_emails(text: str) -> str:
    if not text:
        return ""
    s = remove_invisibles(text)

    # защита от прилипания односимвольных маркеров перед email
    s = _RX_PROTECT.sub("", s)

    # де-гипенизация переносов (g-\nmail → gmail)
    s = _RX_DEHYPHEN.sub(r"\1\2", s)

    # склейка без дефиса — буква на новой строке
    s = _RX_JOIN_NOHYPHEN.sub(r"\1\2@", s)
    s = _RX_JOIN_DOT.sub(r"\1\2\3@", s)

    # новый кейс: слово на строке + ЧИСЛА на следующей + (возможные) пробелы перед '@'
    s = _RX_JOIN_NUM.sub(r"\1\2@", s)

    # \r/\n -> пробел
    s = _RX_CRLF.sub(" ", s)

    # убрать пробелы вокруг '@' и точки
    s = _RX_AT.sub("@", s)
    s = _RX_DOT.sub(r"\1.\2", s)

    # '. c o m' / '. r u'
    s = _RX_DOT_COM.sub(".com", s)
    s = _RX_DOT_RU.sub(".ru", s)

    # '@gmail.co' → '@gmail.com' (и др. провайдеры)
    s = _RX_PROV1.sub(r"\1m", s)
    s = _RX_PROV2.sub(r"\1m", s)

    # разделим «слипшийся хвост» после .ru/.com
    s = _RX_SUFFIX.sub(r"\1 ", s)

    return s


# ---------------- Извлечение email ----------------
def extract_emails_loose(text: str) -> List[str]:
    if not text:
        return []
    s = _preclean_text_for_emails(text)
    rx = re.compile(r"([A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.[A-Za-z]{2,})")
    return [normalize_email(x) for x in rx.findall(s)]


def collapse_footnote_variants(emails: set[str]) -> set[str]:
    if not emails:
        return set()
    base = {re.sub(r"^\.+", "", normalize_email(e)) for e in emails}
    by_suffix: dict[str, set[str]] = {}
    prefix_of: dict[str, str] = {}
    for e in list(base):
        m_num = re.match(r"^(\d{1,2})([A-Za-z][A-Za-z0-9._%+-]*@.+)$", e, flags=re.I)
        if m_num:
            by_suffix.setdefault(m_num.group(2), set()).add(e)
            prefix_of[e] = m_num.group(1)
            continue
        m_chr = re.match(r"^([A-Za-z])([A-Za-z][A-Za-z0-9._%+-]*@.+)$", e, flags=re.I)
        if m_chr:
            by_suffix.setdefault(m_chr.group(2), set()).add(e)
            prefix_of[e] = m_chr.group(1)
            continue
    keep = set(base)
    for suffix, variants in by_suffix.items():
        clean_present = suffix in keep
        distinct_pfx = set(prefix_of[v] for v in variants if v in prefix_of)
        if clean_present or len(distinct_pfx) >= 2:
            keep.difference_update(variants)
            keep.add(suffix)
    return keep


def extract_clean_emails_from_text(text: str) -> Set[str]:
    if not text:
        return set()
    text = _preclean_text_for_emails(text)
    base_re = re.compile(
        rf"([A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.(?:{ALLOWED_TLD_PATTERN}))(?=[^\w]|$)"
    )
    raw = set(base_re.findall(text))
    if not raw:
        return set()
    result: Set[str] = {re.sub(r"^\.+", "", e) for e in raw}
    result = collapse_footnote_variants(result)
    result = {e for e in result if is_allowed_tld(e)}
    return result


def is_numeric_localpart(email_addr: str) -> bool:
    e = normalize_email(email_addr)
    return "@" in e and e.split("@", 1)[0].isdigit()


# ---------- Поиск/устранение усечённых адресов ----------
def detect_numeric_truncations(candidates: Set[str]) -> List[tuple[str, str]]:
    by_key: Dict[tuple[str, str], Set[str]] = {}
    for e in candidates:
        loc, dom = e.split("@", 1)
        m = re.match(r"^([a-z]+)(\d{1,6})$", loc)
        if m:
            key = (m.group(2), dom)
            by_key.setdefault(key, set()).add(e)

    pairs: List[tuple[str, str]] = []
    for e in list(candidates):
        loc, dom = e.split("@", 1)
        if loc.isdigit():
            key = (loc, dom)
            fulls = by_key.get(key, set())
            if len(fulls) == 1:
                good = next(iter(fulls))
                pairs.append((e, good))
    return pairs


def apply_numeric_truncation_removal(allowed_set: Set[str]) -> Tuple[Set[str], List[tuple[str, str]]]:
    pairs = detect_numeric_truncations(allowed_set)
    if not pairs:
        return allowed_set, []
    cleaned = set(allowed_set)
    for bad, _ in pairs:
        cleaned.discard(bad)
    return cleaned, pairs


# --- извлечение из разных форматов ---
def _cached_pdf_text(path: str) -> str:
    try:
        st = os.stat(path)
        key = f"{st.st_size}_{int(st.st_mtime)}"
        cache_path = _PDF_CACHE / key
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8")
        doc = fitz.open(path)
        texts = [page.get_text() or "" for page in doc]
        doc.close()
        joined = " ".join(texts)
        cache_path.write_text(joined, encoding="utf-8")
        return joined
    except Exception as e:
        log_error(f"_cached_pdf_text: {path}: {e}")
        return ""


def _extract_from_pdf(path: str) -> Tuple[Set[str], Set[str]]:
    joined = _cached_pdf_text(path)
    loose = set(extract_emails_loose(joined))
    allowed = set(extract_clean_emails_from_text(joined))
    return allowed, loose


def _extract_from_docx(path: str) -> Tuple[Set[str], Set[str]]:
    doc = Document(path)
    full_text = "\n".join([para.text for para in doc.paragraphs])
    loose = set(extract_emails_loose(full_text))
    allowed = set(extract_clean_emails_from_text(full_text))
    return allowed, loose


def _extract_from_excel(path: str) -> Tuple[Set[str], Set[str]]:
    emails_allowed, emails_loose = set(), set()
    try:
        df = pd.read_excel(path, dtype=str)
        for col in df.columns:
            for val in df[col].dropna():
                s = str(val)
                emails_allowed.update(extract_clean_emails_from_text(s))
                emails_loose.update(extract_emails_loose(s))
    except Exception as e:
        log_error(f"extract_from_excel: {path}: {e}")
    return emails_allowed, emails_loose


def _extract_from_csv(path: str) -> Tuple[Set[str], Set[str]]:
    emails_allowed, emails_loose = set(), set()
    try:
        df = pd.read_csv(path, header=None, dtype=str)
        for col in df.columns:
            for val in df[col].dropna():
                s = str(val)
                emails_allowed.update(extract_clean_emails_from_text(s))
                emails_loose.update(extract_emails_loose(s))
    except Exception as e:
        log_error(f"extract_from_csv: {path}: {e}")
    return emails_allowed, emails_loose


def extract_from_uploaded_file(path: str) -> Tuple[Set[str], Set[str]]:
    p = path.lower()
    if p.endswith(".pdf"):
        return _extract_from_pdf(path)
    if p.endswith(".xlsx"):
        return _extract_from_excel(path)
    if p.endswith(".csv"):
        return _extract_from_csv(path)
    if p.endswith(".docx"):
        return _extract_from_docx(path)
    return set(), set()


async def async_extract_emails_from_url(
    url: str, session, chat_id: int | None = None
):
    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status >= 400:
                log_error(
                    f"async_extract_emails_from_url: {url}: HTTP {resp.status}"
                )
                return (url, [], [], [])
            html_text = await resp.text()
            allowed = extract_clean_emails_from_text(html_text)
            loose = set(extract_emails_loose(html_text))
            foreign = {e for e in loose if not is_allowed_tld(e)}
            repairs = find_prefix_repairs(html_text)
            return (url, list(allowed), list(foreign), repairs)
    except Exception as e:
        log_error(f"async_extract_emails_from_url: {url}: {e}")
        return (url, [], [], [])


# ---------- Repairs ----------
def _remove_invisibles_keep_newlines(text: str) -> str:
    if not text:
        return ""
    return remove_invisibles(text)


def find_prefix_repairs(raw_text: str) -> List[tuple[str, str]]:
    if not raw_text:
        return []
    s = _remove_invisibles_keep_newlines(raw_text)
    pairs, seen = [], set()

    for m in _PAT_A.finditer(s):
        left, rest, dom = m.group(1).lower(), m.group(2).lower(), m.group(3).lower()
        bad, good = f"{rest}@{dom}", f"{left}{rest}@{dom}"
        if (bad, good) not in seen:
            seen.add((bad, good))
            pairs.append((bad, good))

    for m in _PAT_B.finditer(s):
        word, digits, dom = m.group(1).lower(), m.group(2), m.group(3).lower()
        bad, good = f"{digits}@{dom}", f"{word}{digits}@{dom}"
        if (bad, good) not in seen:
            seen.add((bad, good))
            pairs.append((bad, good))

    return pairs


def collect_repairs_from_files(file_paths: List[str]) -> List[tuple[str, str]]:
    repairs: List[tuple[str, str]] = []
    for path in file_paths:
        p = path.lower()
        try:
            if p.endswith(".pdf"):
                doc = fitz.open(path)
                try:
                    raw = "\n".join((pg.get_text() or "") for pg in doc)
                finally:
                    doc.close()
                repairs.extend(find_prefix_repairs(raw))
            elif p.endswith(".docx"):
                doc = Document(path)
                raw = "\n".join(para.text for para in doc.paragraphs)
                repairs.extend(find_prefix_repairs(raw))
        except Exception as e:
            log_error(f"collect_repairs_from_files: {path}: {e}")
    uniq = list(dict.fromkeys(repairs))
    return uniq


def extract_emails_multithreaded(file_paths: List[str]) -> Tuple[Set[str], Set[str]]:
    allowed_all, loose_all = set(), set()

    def process(file):
        try:
            return extract_from_uploaded_file(file)
        except Exception as ex:
            log_error(f"extract_emails_multithreaded:{file}: {ex}")
            return set(), set()

    max_workers = min(32, (os.cpu_count() or 1) * 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for allowed, loose in executor.map(process, file_paths):
            allowed_all.update(allowed)
            loose_all.update(loose)
    return allowed_all, loose_all


async def extract_emails_from_zip(
    zip_path: str, progress_msg, download_dir: str
) -> Tuple[Set[str], List[str], Set[str]]:
    all_allowed: Set[str] = set()
    all_loose: Set[str] = set()
    extracted_files: List[str] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        file_list = [
            f for f in z.namelist() if f.lower().endswith((".pdf", ".xlsx", ".csv", ".docx"))
        ]
        total_files = len(file_list)
        if total_files == 0:
            if progress_msg:
                await progress_msg.edit_text("❌ В архиве не найдено поддерживаемых файлов.")
            return all_allowed, extracted_files, all_loose
        if progress_msg:
            await progress_msg.edit_text(
                f"В архиве {total_files} файлов. Начинаем обработку..."
            )
        for idx, inner_file in enumerate(file_list, 1):
            extracted_path = os.path.join(download_dir, inner_file)
            os.makedirs(os.path.dirname(extracted_path), exist_ok=True)
            z.extract(inner_file, download_dir)
            extracted_files.append(extracted_path)
            allowed, loose = extract_from_uploaded_file(extracted_path)
            all_allowed.update(allowed)
            all_loose.update(loose)
            if progress_msg:
                await progress_msg.edit_text(
                    f"🔄 Обработка файла {idx}/{total_files}:\n{inner_file}"
                )
    return all_allowed, extracted_files, all_loose


__all__ = [
    "normalize_email",
    "is_allowed_tld",
    "strip_html",
    "sample_preview",
    "extract_emails_loose",
    "collapse_footnote_variants",
    "extract_clean_emails_from_text",
    "is_numeric_localpart",
    "detect_numeric_truncations",
    "apply_numeric_truncation_removal",
    "extract_from_uploaded_file",
    "async_extract_emails_from_url",
    "find_prefix_repairs",
    "collect_repairs_from_files",
    "extract_emails_multithreaded",
    "extract_emails_from_zip",
]

