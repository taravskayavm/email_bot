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
from html import unescape
from typing import List, Tuple

__all__ = [
    "strip_html",
    "extract_emails_document",
    "extract_emails_manual",
    "smart_extract_emails",
    "normalize_email",
]


def normalize_email(s: str) -> str:
    return (s or "").strip().lower()


# ====================== НОРМАЛИЗАЦИЯ ТЕКСТА ======================

_Z_SPACE_RE = re.compile(r"[\u2000-\u200A\u202F\u205F\u3000]")  # тонкие/узкие/идеографические пробелы
_BULLETS = "•·⋅◦"
_BRACKETS_OPEN = "([{〔【〈《"
_BRACKETS_CLOSE = ")]}\u3015\u3011\u3009\u300B"

def _normalize_typography(s: str) -> str:
    # Юникодная нормализация
    s = unicodedata.normalize("NFKC", s or "")
    # Пробелы
    s = s.replace("\u00A0", " ")  # NBSP
    s = _Z_SPACE_RE.sub(" ", s)  # Z* пробелы -> обычный пробел
    # Нулевой ширины, BOM, мягкий перенос
    s = (s.replace("\u200B", "").replace("\u200C", "").replace("\u200D", "")
           .replace("\uFEFF", "").replace("\u00AD", ""))
    # Тире/минусы к ASCII '-'
    s = (s.replace("\u2010", "-").replace("\u2011", "-").replace("\u2012", "-")
           .replace("\u2013", "-").replace("\u2014", "-").replace("\u2015", "-")
           .replace("\u2212", "-").replace("\u2043", "-").replace("\uFE63", "-")
           .replace("\uFF0D", "-"))
    # Апострофы/кавычки к ASCII "'"
    s = (s.replace("\u2018", "'").replace("\u2019", "'").replace("\u2032", "'")
           .replace("\uFF07", "'"))
    # Полноширинные знаки
    s = s.replace("\uFF20", "@").replace("\uFF0E", ".")
    return s

def _preprocess_text(text: str) -> str:
    text = _normalize_typography(text)
    # Склейка переносов внутри адресов (сохраняем дефис/точку и др. atext)
    atext = "A-Za-z0-9!#$%&'*+/=?^_`{|}~.-"
    text = re.sub(fr"([{atext}])-\n([{atext}])", r"\1-\2", text)
    text = re.sub(fr"([{atext}])\n([{atext}])", r"\1\2", text)
    return text

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
    s = _normalize_typography(html)
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
    text = _preprocess_text(text)
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

        if len(local) >= 2 and _is_left_boundary(left_char):
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

    s = _preprocess_text(text)
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

