# -*- coding: utf-8 -*-
"""
Извлечение e-mail и очистка HTML, без внешних зависимостей (офлайн).

Публичные функции:
- strip_html(html: str) -> str
- extract_emails_document(text: str) -> list[str]
- extract_emails_manual(text: str) -> list[str]
"""

from __future__ import annotations
import re
import unicodedata
from dataclasses import dataclass
from html import unescape
from typing import List, Tuple, Dict, Iterable, Set, Optional

from . import settings
from .dedupe import merge_footnote_prefix_variants
from .extraction_common import normalize_email, normalize_text, preprocess_text
from .extraction_pdf import extract_from_pdf, extract_from_pdf_stream
from .extraction_zip import extract_emails_from_zip
from .settings_store import get

__all__ = [
    "EmailHit",
    "strip_html",
    "extract_emails_document",
    "extract_emails_manual",
    "smart_extract_emails",
    "normalize_email",
    "extract_from_pdf",
    "extract_from_docx",
    "extract_from_xlsx",
    "extract_from_csv_or_text",
    "extract_emails_from_zip",
    "extract_from_url",
    "extract_any",
    "extract_any_stream",
]


@dataclass(frozen=True)
class EmailHit:
    email: str           # нормализованный e-mail
    source_ref: str      # pdf:/path.pdf#page=5 | url:https://... | zip:/a.zip|inner.pdf#page=2 | xlsx:/file.xlsx!Лист1:B12
    origin: str          # 'mailto' | 'direct_at' | 'obfuscation' | 'cfemail'
    pre: str = ""        # до 16 символов слева от совпадения в исходном тексте
    post: str = ""       # до 16 символов справа


_BULLETS = "•·⋅◦"
_BRACKETS_OPEN = "([{〔【〈《"
_BRACKETS_CLOSE = ")]}\u3015\u3011\u3009\u300B"

# ====================== STRIP HTML ======================

_SCRIPT_STYLE_RE = re.compile(r"(?is)<(script|style)\b[^>]*>.*?</\1>")
_TAG_RE = re.compile(r"(?s)<[^>]+>")
_BR_RE = re.compile(r"(?is)<br\s*/?>")
_P_BLOCK_RE = re.compile(r"(?is)</?(p|div|tr|h[1-6]|table|ul|ol)\b[^>]*>")
_LI_RE = re.compile(r"(?is)<li\b[^>]*>")

def strip_html(html: str) -> str:
    """
    Удаляет HTML-разметку:
    - script/style блоки;
    - переводит <br> -> \n, <p>/<div>/<tr>/<h1..6>/<table>/<ul>/<ol> -> \n;
    - <li> -> '\n- ';
    - снимает остальные теги;
    - декодирует HTML-сущности; схлопывает пробелы/пустые строки.
    """
    if not html:
        return ""
    s = normalize_text(html)
    s = _SCRIPT_STYLE_RE.sub("\n", s)
    s = _BR_RE.sub("\n", s)
    s = _LI_RE.sub("\n- ", s)
    s = _P_BLOCK_RE.sub("\n", s)
    s = _TAG_RE.sub(" ", s)  # снять остальные теги
    s = unescape(s)
    s = s.replace("\r", "")
    # NBSP (после unescape) -> пробел
    s = s.replace("\xa0", " ")
    # Схлопывание пробелов и пустых строк
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

# ====================== ПОМОЩНИКИ ДЛЯ E-MAIL ======================

_ATEXT_PUNCT = set("!#$%&'*+/=?^_`{|}~.-")  # RFC 5322 atext (включая '.' и '-')

def _is_local_char(ch: str) -> bool:
    return ch.isalnum() or ch in _ATEXT_PUNCT

def _valid_local(local: str) -> bool:
    if not local or local[0] == "." or local[-1] == "." or ".." in local:
        return False
    return all(_is_local_char(c) for c in local)

def _valid_domain(domain: str) -> bool:
    parts = domain.split(".")
    if len(parts) < 2:
        return False
    for label in parts:
        if not label or label[0] == "-" or label[-1] == "-":
            return False
        # доменная метка — ASCII alnum или '-'
        if not all(c.isalnum() or c == "-" for c in label):
            return False
    tld = parts[-1]
    if tld.startswith("xn--"):
        return 4 <= len(tld) <= 63
    return tld.isalpha() and 2 <= len(tld) <= 63

def _scan_local_left(text: str, at_idx: int) -> Tuple[str, int]:
    i = at_idx - 1
    buf = []
    while i >= 0 and _is_local_char(text[i]):
        buf.append(text[i]); i -= 1
    return "".join(reversed(buf)), i  # i — индекс символа слева от local (или -1)

def _scan_domain_right(text: str, at_idx: int) -> str:
    n, j = len(text), at_idx + 1
    labels: list[str] = []
    while j < n:
        if j >= n or not text[j].isalnum():
            break
        start = j
        j += 1
        while j < n and (text[j].isalnum() or text[j] == "-"):
            j += 1
        label = text[start:j]
        if not label or label.endswith("-"):
            break
        labels.append(label)
        if j < n and text[j] == ".":
            j += 1
            continue
        else:
            break
    if len(labels) < 2:
        return ""
    return ".".join(labels)

# ====================== ОБРЕЗКА TLD ======================

_COMMON_TLDS = {
    # generic + популярные
    "com","org","net","edu","gov","mil","info","biz","name","pro","int",
    "aero","coop","museum","travel","mobi","online","site","agency","app","dev","io","ai",
    # ccTLD
    "ru","su","by","kz","ua","uk","us","ca","de","fr","it","pl","cz","sk","ch","se","no","fi",
    "es","pt","nl","be","tr","ge","az","am","kg","uz","tj","tm","cn","jp","kr","lt","lv","ee",
    "in","br","ar","au","nz","at","dk","gr","hu","ro","rs","bg","md","il","ie","hk","sg","my",
    "id","th","vn","pk","ae","qa","sa","eg","ma","tn","al","mk","ba","hr","si","me","is","li",
    "za","ng","ke"
}

def _longest_known_tld_prefix(s: str) -> str | None:
    s = s.lower()
    best = None
    for t in _COMMON_TLDS:
        if s.startswith(t) and (best is None or len(t) > len(best)):
            best = t
    return best

def _trim_appended_word(domain: str) -> str:
    """
    Укоротить последний ярлык до валидного TLD в случаях:
      - 'rurussia' -> 'ru'; 'edua' -> 'edu'; 'ru2020','ru_abc','ru-abc' -> 'ru'
      - повторы 'ruru','comcom','comcomcom' -> один раз
      - 'onlinebiz' -> 'online'
    """
    parts = domain.split(".")
    last = parts[-1]
    if last.startswith("xn--"):
        return domain

    t = last.lower()
    if t in _COMMON_TLDS:
        return domain

    # Повтор TLD (2+ раза): comcom[com], ruru, comcomcom
    for base in sorted(_COMMON_TLDS, key=len, reverse=True):
        if len(t) >= 2*len(base) and t == base * (len(t)//len(base)):
            parts[-1] = base
            return ".".join(parts)

    # base + хвост (буквы/цифры/_/-) длиной 1..10
    m = re.match(r"^([a-z]{2,})([A-Za-z0-9_-]{1,10})$", t)
    if m:
        base = m.group(1)
        pref = _longest_known_tld_prefix(base)
        if pref:
            parts[-1] = pref
            return ".".join(parts)

    # Максимальный известный префикс (onlinebiz -> online)
    pref = _longest_known_tld_prefix(t)
    if pref:
        parts[-1] = pref
        return ".".join(parts)

    return domain

# ====================== ГРАНИЦЫ/ПРЕФИКСЫ ======================

def _is_left_boundary(ch: str | None) -> bool:
    if ch is None:
        return True
    if ch.isalnum():
        return False
    # Символы «склейки» local-part НЕ считаем границей
    if ch in "._%+-'~=/":
        return False
    cat = unicodedata.category(ch)  # Z* (separators), P* (punctuation)
    if cat.startswith("Z") or cat.startswith("P"):
        return True
    if ch in _BULLETS or ch in _BRACKETS_OPEN + _BRACKETS_CLOSE:
        return True
    return False

_LIST_MARKER_RE = re.compile(
    rf"(?m)[\s{re.escape(_BULLETS)}{re.escape(_BRACKETS_OPEN)}]"
    r"[A-Za-z0-9][\)\.\:]\s+$"
)

def _multi_prefix_mode(text: str) -> bool:
    """
    «Ряд префиксов» по документу:
    True, если >=3 маркеров перед адресами, или >=2 разных префикса, каждый >=2 раз.
    """
    counts, total = {}, 0
    for m in re.finditer(r"(?m)(.)([A-Za-z0-9])([A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]+)@", text):
        left, pref = m.group(1), m.group(2)
        if _is_left_boundary(left):
            counts[pref] = counts.get(pref, 0) + 1
            total += 1
    if total >= 3:
        return True
    return sum(1 for v in counts.values() if v >= 2) >= 2

# ====================== ОСНОВНАЯ ФУНКЦИЯ ======================

def smart_extract_emails(text: str) -> List[str]:
    """
    Возвращает список e-mail из «грязного» текста (PDF/ZIP), очищая:
    - префиксные сноски (1/a/б/… без скобок) перед адресами;
    - «пришитые» слова/хвосты после TLD;
    - переносы строк и типографику внутри адресов.
    Не режет валидный local-part (поддержаны все символы RFC atext).
    """
    text = preprocess_text(text)
    low_text = text.lower()
    multi_mode = _multi_prefix_mode(text)

    # Словарь «похожих на почту» форм (для скоринга V2 независимо от порядка)
    seen_in_text = set(m.group(0) for m in re.finditer(
        r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]+@[A-Za-z0-9.-]+", low_text
    ))

    emails: list[str] = []
    i, n = 0, len(text)
    while True:
        at = text.find("@", i)
        if at == -1:
            break

        local, left_idx = _scan_local_left(text, at)
        domain_raw = _scan_domain_right(text, at)
        domain = domain_raw
        if not local or not domain:
            i = at + 1
            continue

        domain = _trim_appended_word(domain)

        # Вариант V1 — как есть
        email_v1 = f"{local}@{domain}".lower()

        # Вариант V2 — снять 1 префикс (если слева граница и после снятия local валиден)
        choose_v2 = False
        email_v2 = email_v1
        left_char = text[left_idx] if left_idx >= 0 else None

        if local.isdigit():
            choose_v2 = False
        elif len(local) >= 2 and _is_left_boundary(left_char):
            prefix_char = local[0]
            local2 = local[1:]
            if _valid_local(local2) and (prefix_char.isdigit() or prefix_char.islower()):
                email_v2 = f"{local2}@{domain}".lower()
                # --- скоринг ---
                score_v1 = 0
                score_v2 = 0

                email_raw = f"{local}@{domain_raw}".lower()
                if email_raw in seen_in_text: score_v1 += 2
                if email_v2 in seen_in_text: score_v2 += 2

                if email_v2 in emails: score_v2 += 3       # уже видели без префикса -> сильный сигнал
                if prefix_char.isdigit():
                    score_v2 += 4    # цифры чаще сноски
                elif prefix_char.lower() in {"a", "b", "c"}:
                    score_v2 += 3    # буквенные сноски a/b/c

                # проверим шаблон списка непосредственно слева
                left_slice_start = max(0, at - len(local) - 4)
                left_slice = text[left_slice_start: at - len(local)]
                if _LIST_MARKER_RE.search(left_slice):
                    score_v2 += 4

                if multi_mode: score_v2 += 2               # «ряд префиксов» по документу
                if len(local2) >= 2: score_v2 += 1
                if len(local)  >= 2: score_v1 += 1

                choose_v2 = score_v2 > score_v1

        final_email = email_v2 if choose_v2 else email_v1
        loc, dom = final_email.split("@", 1)
        if _valid_local(loc) and _valid_domain(dom):
            emails.append(final_email)

        i = at + 1

    # Дедуп с сохранением порядка
    out, seen = [], set()
    for e in emails:
        if e not in seen:
            out.append(e); seen.add(e)
    return out


# --- MANUAL mode (for chat input) ---------------------------------

_EMAIL_CORE = r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,63}"
_RE_ANGLE = re.compile(rf"<\s*({_EMAIL_CORE})\s*>")
_RE_MAILTO = re.compile(rf"mailto:\s*({_EMAIL_CORE})", re.IGNORECASE)
_RE_RAW = re.compile(rf"(?<![A-Za-z0-9._%+-])({_EMAIL_CORE})(?![A-Za-z0-9-])")

_TRAIL_PUNCT = ".,;:!?)”’»"


def _strip_trailing_punct(addr: str) -> str:
    while addr and addr[-1] in _TRAIL_PUNCT:
        addr = addr[:-1]
    return addr


def extract_emails_manual(text: str) -> list[str]:
    """
    Консервативный парсер для ручного ввода в чате.
    Понимает <email>, mailto:, разделители и терминальную пунктуацию.
    НЕ снимает «префиксы-сноски».
    """
    if not text:
        return []

    s = preprocess_text(text)
    s_low = s.lower()

    found: list[str] = []

    for m in _RE_ANGLE.finditer(s_low):
        found.append(_strip_trailing_punct(m.group(1)))
    for m in _RE_MAILTO.finditer(s_low):
        found.append(_strip_trailing_punct(m.group(1)))
    for m in _RE_RAW.finditer(s_low):
        found.append(_strip_trailing_punct(m.group(1)))

    out, seen = [], set()
    for e in found:
        e = e.strip().lower()
        if not e:
            continue
        try:
            local, dom = e.split("@", 1)
        except ValueError:
            continue
        if _valid_local(local) and _valid_domain(dom):
            if e not in seen:
                out.append(e); seen.add(e)
    return out


# Чтобы сохранить обратную совместимость
def extract_emails_document(text: str) -> list[str]:
    return smart_extract_emails(text)


# ====================== ФАЙЛЫ И САЙТЫ ======================

from typing import Dict, Iterable, Optional, Set


def _dedupe(hits: Iterable[EmailHit]) -> list[EmailHit]:
    seen: Set[str] = set()
    out: list[EmailHit] = []
    for h in hits:
        n = normalize_email(h.email)
        if n and n not in seen:
            seen.add(n)
            if n == h.email:
                out.append(h)
            else:
                out.append(
                    EmailHit(
                        email=n,
                        source_ref=h.source_ref,
                        origin=h.origin,
                        pre=h.pre,
                        post=h.post,
                    )
                )
    return out


def extract_from_docx(path: str, stop_event: Optional[object] = None) -> tuple[list[EmailHit], Dict]:
    """Извлечь e-mail-адреса из DOCX, учитывая номера страниц."""

    import re
    import zipfile
    import xml.etree.ElementTree as ET

    hits: List[EmailHit] = []
    try:
        with zipfile.ZipFile(path) as z:
            xml = z.read("word/document.xml")
    except Exception:
        return [], {"errors": ["cannot open"]}

    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    try:
        root = ET.fromstring(xml)
    except Exception:
        return [], {"errors": ["cannot open"]}

    page = 1
    text = ""
    stats: Dict[str, int] = {"pages": 0}

    def flush(page_text: str, page_no: int) -> None:
        low = page_text.lower()
        for email in extract_emails_document(page_text):
            for m in re.finditer(re.escape(email), low):
                start, end = m.span()
                pre = page_text[max(0, start - 16) : start]
                post = page_text[end : end + 16]
                hits.append(
                    EmailHit(
                        email=email,
                        source_ref=f"docx:{path}#page={page_no}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )

    for elem in root.iter():
        if elem.tag == ns + "br" and elem.attrib.get(ns + "type") == "page":
            flush(text, page)
            page += 1
            text = ""
        elif elem.tag == ns + "lastRenderedPageBreak":
            flush(text, page)
            page += 1
            text = ""
        elif elem.tag == ns + "t":
            text += elem.text or ""
        elif elem.tag == ns + "p":
            text += "\n"

    flush(text, page)
    stats["pages"] = page
    return _dedupe(hits), stats


def extract_from_xlsx(path: str, stop_event: Optional[object] = None) -> tuple[list[EmailHit], Dict]:
    """Извлечь e-mail-адреса из XLSX."""

    try:
        import openpyxl  # type: ignore
        from openpyxl.utils import get_column_letter  # type: ignore

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        hits: List[EmailHit] = []
        cells = 0
        try:
            for ws in wb.worksheets:
                for r_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        break
                    for c_idx, val in enumerate(row, 1):
                        cells += 1
                        if isinstance(val, str):
                            for e in extract_emails_document(val):
                                coord = f"{get_column_letter(c_idx)}{r_idx}"
                                ref = f"xlsx:{path}!{ws.title}:{coord}"
                                hits.append(
                                    EmailHit(email=e, source_ref=ref, origin="direct_at")
                                )
        finally:
            try:
                wb.close()
            except Exception:
                pass
        return _dedupe(hits), {"cells": cells}
    except Exception:
        # Fallback: parse XML inside zip
        import zipfile
        import re

        hits: List[EmailHit] = []
        cells = 0
        try:
            with zipfile.ZipFile(path) as z:
                for name in z.namelist():
                    if not name.startswith("xl/") or not name.endswith(".xml"):
                        continue
                    xml = z.read(name).decode("utf-8", "ignore")
                    for txt in re.findall(r">([^<>]+)<", xml):
                        cells += 1
                        for e in extract_emails_document(txt):
                            hits.append(EmailHit(email=e, source_ref=f"xlsx:{path}", origin="direct_at"))
        except Exception:
            return [], {"errors": ["cannot open"]}
        return _dedupe(hits), {"cells": cells}


def extract_from_csv_or_text(path: str, stop_event: Optional[object] = None) -> tuple[list[EmailHit], Dict]:
    """Извлечь e-mail из CSV или текстового файла."""

    import os
    import csv

    hits: List[EmailHit] = []
    lines = 0
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            with open(path, newline="", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f)
                for row in reader:
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        break
                    lines += 1
                    for cell in row:
                        s = str(cell)
                        for e in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", s):
                            hits.append(
                                EmailHit(
                                    email=e,
                                    source_ref=f"{ext.lstrip('.')}:{path}",
                                    origin="direct_at",
                                )
                            )
        else:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        break
                    lines += 1
                    for e in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", line):
                        hits.append(
                            EmailHit(
                                email=e,
                                source_ref=f"{ext.lstrip('.')}:{path}",
                                origin="direct_at",
                            )
                        )
    except Exception:
        return [], {"errors": ["cannot open"]}
    return _dedupe(hits), {"lines": lines}


def extract_from_docx_stream(
    data: bytes, source_ref: str, stop_event: Optional[object] = None
) -> tuple[list[EmailHit], Dict]:
    import io
    import re
    import zipfile
    import xml.etree.ElementTree as ET

    hits: List[EmailHit] = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            xml = z.read("word/document.xml")
    except Exception:
        return [], {"errors": ["cannot open"]}

    ns = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    try:
        root = ET.fromstring(xml)
    except Exception:
        return [], {"errors": ["cannot open"]}

    page = 1
    text = ""
    stats: Dict[str, int] = {"pages": 0}

    def flush(page_text: str, page_no: int) -> None:
        low = page_text.lower()
        for email in extract_emails_document(page_text):
            for m in re.finditer(re.escape(email), low):
                start, end = m.span()
                pre = page_text[max(0, start - 16) : start]
                post = page_text[end : end + 16]
                hits.append(
                    EmailHit(
                        email=email,
                        source_ref=f"{source_ref}#page={page_no}",
                        origin="direct_at",
                        pre=pre,
                        post=post,
                    )
                )

    for elem in root.iter():
        if elem.tag == ns + "br" and elem.attrib.get(ns + "type") == "page":
            flush(text, page)
            page += 1
            text = ""
        elif elem.tag == ns + "lastRenderedPageBreak":
            flush(text, page)
            page += 1
            text = ""
        elif elem.tag == ns + "t":
            text += elem.text or ""
        elif elem.tag == ns + "p":
            text += "\n"

    flush(text, page)
    stats["pages"] = page
    return _dedupe(hits), stats


def extract_from_xlsx_stream(
    data: bytes, source_ref: str, stop_event: Optional[object] = None
) -> tuple[list[EmailHit], Dict]:
    import io

    try:
        import openpyxl  # type: ignore
        from openpyxl.utils import get_column_letter  # type: ignore

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        hits: List[EmailHit] = []
        cells = 0
        try:
            for ws in wb.worksheets:
                for r_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        break
                    for c_idx, val in enumerate(row, 1):
                        cells += 1
                        if isinstance(val, str):
                            for e in extract_emails_document(val):
                                coord = f"{get_column_letter(c_idx)}{r_idx}"
                                ref = f"{source_ref}!{ws.title}:{coord}"
                                hits.append(
                                    EmailHit(email=e, source_ref=ref, origin="direct_at")
                                )
        finally:
            try:
                wb.close()
            except Exception:
                pass
        return _dedupe(hits), {"cells": cells}
    except Exception:
        import re
        import zipfile

        hits: List[EmailHit] = []
        cells = 0
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                for name in z.namelist():
                    if not name.startswith("xl/") or not name.endswith(".xml"):
                        continue
                    xml = z.read(name).decode("utf-8", "ignore")
                    for txt in re.findall(r">([^<>]+)<", xml):
                        cells += 1
                        for e in extract_emails_document(txt):
                            hits.append(
                                EmailHit(email=e, source_ref=source_ref, origin="direct_at")
                            )
        except Exception:
            return [], {"errors": ["cannot open"]}
        return _dedupe(hits), {"cells": cells}


def extract_from_csv_or_text_stream(
    data: bytes, ext: str, source_ref: str, stop_event: Optional[object] = None
) -> tuple[list[EmailHit], Dict]:
    import csv
    import io
    import re

    pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    hits: List[EmailHit] = []
    lines = 0
    text = data.decode("utf-8", "ignore")
    if ext == ".csv":
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            lines += 1
            for cell in row:
                s = str(cell)
                for e in re.findall(pattern, s):
                    hits.append(EmailHit(email=e, source_ref=source_ref, origin="direct_at"))
    else:
        for line in io.StringIO(text):
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            lines += 1
            for e in re.findall(pattern, line):
                hits.append(EmailHit(email=e, source_ref=source_ref, origin="direct_at"))
    return _dedupe(hits), {"lines": lines}


from .extraction_url import extract_obfuscated_hits, fetch_url, decode_cfemail


def extract_from_html_stream(
    data: bytes, source_ref: str, stop_event: Optional[object] = None
) -> tuple[list[EmailHit], Dict]:
    import re
    import urllib.parse

    html = data.decode("utf-8", "ignore")
    hits: List[EmailHit] = []
    stats: Dict[str, int] = {
        "urls_scanned": 1,
        "cfemail_decoded": 0,
        "obfuscated_hits": 0,
        "numeric_from_obfuscation_dropped": 0,
    }
    for m in re.finditer(r'href=["\']mailto:([^"\'?]+)', html, flags=re.I):
        addr = urllib.parse.unquote(m.group(1))
        hits.append(EmailHit(email=addr.lower(), source_ref=source_ref, origin="mailto"))
    for cf in re.findall(r'data-cfemail="([0-9a-fA-F]+)"', html):
        try:
            email = decode_cfemail(cf)
        except Exception:
            continue
        hits.append(EmailHit(email=email, source_ref=source_ref, origin="cfemail"))
        stats["cfemail_decoded"] += 1
    text = strip_html(html)
    for e in extract_emails_document(text):
        hits.append(EmailHit(email=e, source_ref=source_ref, origin="direct_at"))
    obf_hits = extract_obfuscated_hits(text, source_ref, stats)
    stats["obfuscated_hits"] = len(obf_hits)
    hits.extend(obf_hits)
    return hits, stats


def extract_from_url(
    url: str, stop_event: Optional[object] = None, *, max_depth: int = 2
) -> tuple[list[EmailHit], Dict]:
    """Загрузить веб-страницу и извлечь e-mail-адреса."""

    settings.STRICT_OBFUSCATION = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    settings.FOOTNOTE_RADIUS_PAGES = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    settings.PDF_LAYOUT_AWARE = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    settings.ENABLE_OCR = get("ENABLE_OCR", settings.ENABLE_OCR)

    import re
    import urllib.parse

    stats: Dict[str, int | list] = {
        "urls_scanned": 0,
        "cfemail_decoded": 0,
        "obfuscated_hits": 0,
        "numeric_from_obfuscation_dropped": 0,
        "errors": [],
    }
    hits: List[EmailHit] = []

    def _process(html: str, current_url: str) -> None:
        source_ref = f"url:{current_url}"
        stats["urls_scanned"] += 1
        for m in re.finditer(r'href=["\']mailto:([^"\'?]+)', html, flags=re.I):
            addr = urllib.parse.unquote(m.group(1))
            hits.append(EmailHit(email=addr.lower(), source_ref=source_ref, origin="mailto"))
        for cf in re.findall(r'data-cfemail="([0-9a-fA-F]+)"', html):
            try:
                email = decode_cfemail(cf)
            except Exception:
                continue
            hits.append(
                EmailHit(
                    email=email,
                    source_ref=source_ref,
                    origin="cfemail",
                )
            )
            stats["cfemail_decoded"] += 1
        text = strip_html(html)
        for e in extract_emails_document(text):
            hits.append(EmailHit(email=e, source_ref=source_ref, origin="direct_at"))
        obf_hits = extract_obfuscated_hits(text, source_ref, stats)
        stats["obfuscated_hits"] += len(obf_hits)
        hits.extend(obf_hits)

    visited: set[str] = set()
    parsed_root = urllib.parse.urlparse(url)

    def _crawl(current_url: str, depth: int) -> None:
        if depth < 0 or current_url in visited:
            return
        html = fetch_url(current_url, stop_event)
        if not html:
            return
        visited.add(current_url)
        _process(html, current_url)
        if depth == 0:
            return
        links = re.findall(r'href=["\']([^"\']+)', html, flags=re.I)
        if len(links) > 30:
            return
        for href in links:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            if not re.search(r"contact|contacts|about|region|regiony|regions|контакт", href, re.I):
                continue
            new = urllib.parse.urljoin(current_url, href)
            parsed = urllib.parse.urlparse(new)
            if parsed.netloc != parsed_root.netloc:
                continue
            _crawl(new, depth - 1)

    _crawl(url, max_depth - 1)
    hits = merge_footnote_prefix_variants(hits, stats)
    return _dedupe(hits), stats

def extract_any(
    source: str,
    stop_event: Optional[object] = None,
    _return_hits: bool = False,
) -> tuple[list[EmailHit] | list[str], Dict]:
    """Определить тип источника и извлечь e-mail-адреса.

    Если ``_return_hits`` истинно, функция возвращает список ``EmailHit``;
    иначе возвращает отсортированный список уникальных адресов.
    """

    settings.STRICT_OBFUSCATION = get("STRICT_OBFUSCATION", settings.STRICT_OBFUSCATION)
    settings.FOOTNOTE_RADIUS_PAGES = get("FOOTNOTE_RADIUS_PAGES", settings.FOOTNOTE_RADIUS_PAGES)
    settings.PDF_LAYOUT_AWARE = get("PDF_LAYOUT_AWARE", settings.PDF_LAYOUT_AWARE)
    settings.ENABLE_OCR = get("ENABLE_OCR", settings.ENABLE_OCR)

    import os
    import re

    if re.match(r"https?://", source, re.I):
        hits, stats = extract_from_url(source, stop_event)
        if _return_hits:
            return hits, stats
        return sorted({h.email for h in hits}), stats

    ext = os.path.splitext(source)[1].lower()
    if ext == ".pdf":
        hits, stats = extract_from_pdf(source, stop_event)
    elif ext == ".docx":
        hits, stats = extract_from_docx(source, stop_event)
    elif ext == ".xlsx":
        hits, stats = extract_from_xlsx(source, stop_event)
    elif ext in {".csv", ".txt"}:
        hits, stats = extract_from_csv_or_text(source, stop_event)
    elif ext == ".zip":
        hits, stats = extract_emails_from_zip(source, stop_event)
    elif ext in {".html", ".htm"}:
        import urllib.parse
        with open(source, encoding="utf-8", errors="ignore") as f:
            html = f.read()
        hits = []
        stats = {
            "urls_scanned": 1,
            "cfemail_decoded": 0,
            "obfuscated_hits": 0,
            "numeric_from_obfuscation_dropped": 0,
        }
        source_ref = f"html:{source}"
        for m in re.finditer(r'href=["\']mailto:([^"\'?]+)', html, flags=re.I):
            addr = urllib.parse.unquote(m.group(1))
            hits.append(EmailHit(email=addr.lower(), source_ref=source_ref, origin="mailto"))
        for cf in re.findall(r'data-cfemail="([0-9a-fA-F]+)"', html):
            try:
                email = decode_cfemail(cf)
            except Exception:
                continue
            hits.append(EmailHit(email=email, source_ref=source_ref, origin="cfemail"))
            stats["cfemail_decoded"] += 1
        text = strip_html(html)
        for e in extract_emails_document(text):
            hits.append(EmailHit(email=e, source_ref=source_ref, origin="direct_at"))
        obf_hits = extract_obfuscated_hits(text, source_ref, stats)
        stats["obfuscated_hits"] = len(obf_hits)
        hits.extend(obf_hits)
    else:
        with open(source, encoding="utf-8", errors="ignore") as f:
            text = f.read()
        hits = [
            EmailHit(email=e, source_ref=f"txt:{source}", origin="direct_at")
            for e in extract_emails_document(text)
        ]
        stats = {}

    hits = merge_footnote_prefix_variants(hits, stats)
    hits = _dedupe(hits)
    if _return_hits:
        return hits, stats
    return sorted({h.email for h in hits}), stats


def extract_any_stream(
    data: bytes,
    ext: str,
    *,
    source_ref: str,
    stop_event: Optional[object] = None,
) -> tuple[list[EmailHit], Dict]:
    """Определить тип источника по расширению и извлечь e-mail из байтов."""

    ext = ext.lower()
    if ext == ".pdf":
        hits, stats = extract_from_pdf_stream(data, source_ref, stop_event)
    elif ext == ".docx":
        hits, stats = extract_from_docx_stream(data, source_ref, stop_event)
    elif ext == ".xlsx":
        hits, stats = extract_from_xlsx_stream(data, source_ref, stop_event)
    elif ext in {".csv", ".txt"}:
        hits, stats = extract_from_csv_or_text_stream(data, ext, source_ref, stop_event)
    elif ext in {".html", ".htm"}:
        hits, stats = extract_from_html_stream(data, source_ref, stop_event)
    else:
        text = data.decode("utf-8", "ignore")
        hits = [
            EmailHit(email=e, source_ref=source_ref, origin="direct_at")
            for e in extract_emails_document(text)
        ]
        stats = {}
    hits = merge_footnote_prefix_variants(hits, stats)
    hits = _dedupe(hits)
    return hits, stats


