import logging
import os
import json
import re
import unicodedata
import string
from pathlib import Path

import idna

from config import CONFUSABLES_NORMALIZE, OBFUSCATION_ENABLE
from utils.email_deobfuscate import deobfuscate_text
from utils.email_role import classify_email_role

from utils.tld_utils import is_allowed_domain
from utils.paths import expand_path, ensure_parent, get_temp_file

logger = logging.getLogger(__name__)
_FOOTNOTES_MODE = (os.getenv("FOOTNOTES_MODE", "smart") or "smart").lower()

# Простые маркеры, указывающие на обфусцированный адрес (например, name(at)domain).
_AUTO_DEOBF_HINTS = re.compile(r"(?i)(\[[^\]]*(?:at|dot)[^\]]*\]|\([^)]*(?:at|dot)[^)]*\))")

# -------- ЕДИНЫЙ ЧИСТИЛЬЩИК НЕВИДИМЫХ СИМВОЛОВ --------
# Удаляем невидимые пробелы/переносы/bi-di маркеры, часто попадающие из PDF/OCR.
# Внимание: чистим исходный текст, НО не лезем внутрь уже матчинговых адресов.
_INVISIBLES_RE = re.compile(
    r"["
    r"\u00AD"  # SOFT HYPHEN
    r"\u200B-\u200F"  # ZWSP..RLM
    r"\u202A-\u202E"  # LRE..RLO/PDF
    r"\u2028\u2029"  # LINE/PARAGRAPH SEPARATOR
    r"\u202F"  # NARROW NO-BREAK SPACE
    r"\u205F"  # MEDIUM MATHEMATICAL SPACE
    r"\u2060-\u206F"  # WORD JOINER..INVISIBLE OPS
    r"\u2066-\u2069"  # LRI/RLI/FSI/PDI
    r"\uFEFF"  # ZERO WIDTH NO-BREAK SPACE (BOM)
    r"\u1680"  # OGHAM SPACE MARK
    r"\u180E"  # MONGOLIAN VOWEL SEPARATOR
    r"]"
)


def strip_invisibles(text: str) -> str:
    """Удаляет невидимые/служебные Unicode-символы, мешающие парсингу."""
    if not text:
        return text
    before = len(text)
    cleaned = _INVISIBLES_RE.sub("", text)
    if len(cleaned) != before:
        try:
            logger.debug(
                "strip_invisibles: removed %d hidden chars", before - len(cleaned)
            )
        except Exception:
            pass
    return cleaned


# Внешняя пунктуация, встречающаяся вокруг e-mail при парсинге
_PUNCT_TRIM_RE = re.compile(
    r'^[\s\(\[\{<«‹"“”„‚’›»>}\]\).,:;—]+|[\s\(\[\{<«‹"“”„‚’›»>}\]\).,:;—]+$'
)

# Цифровые сноски (включая надстрочные ¹²³ и пр. circled numbers)
_SUPERSCRIPT_MAP = str.maketrans(
    {
        "¹": "1",
        "²": "2",
        "³": "3",
        "⁰": "0",
        "⁴": "4",
        "⁵": "5",
        "⁶": "6",
        "⁷": "7",
        "⁸": "8",
        "⁹": "9",
    }
)
# ①②③…⑳ → 1..20 (нужны хотя бы 1–9)
_CIRCLED_MAP = {chr(cp): str(i) for i, cp in enumerate(range(0x2460, 0x2469), start=1)}

# Таблица безопасных кириллических гомоглифов
_CONFUSABLE_TRANSLATION = str.maketrans(
    {
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "у": "y",
        "х": "x",
        "к": "k",
        "м": "m",
        "т": "t",
        "в": "b",
        "н": "h",
        "л": "l",
        "А": "A",
        "Е": "E",
        "О": "O",
        "Р": "P",
        "С": "C",
        "У": "Y",
        "Х": "X",
        "К": "K",
        "М": "M",
        "Т": "T",
        "В": "B",
        "Н": "H",
        "Л": "L",
        "і": "i",
        "І": "I",
        "ј": "j",
        "Ј": "J",
    }
)

_ASCII_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_ASCII_DOMAIN_LABEL_RE = re.compile(r"^[a-z0-9-]+$")

_DEFAULT_DEBUG_LOG = get_temp_file("email_parse_debug.log")


def _debug_log_path() -> Path:
    """Return path for debug logging respecting environment overrides."""

    raw = os.getenv("DEBUG_EMAIL_PARSE_LOG_PATH", str(_DEFAULT_DEBUG_LOG))
    return expand_path(raw)


def _is_cyrillic(ch: str) -> bool:
    try:
        return "CYRILLIC" in unicodedata.name(ch)
    except ValueError:
        return False


def _is_latin(ch: str) -> bool:
    try:
        return "LATIN" in unicodedata.name(ch)
    except ValueError:
        return False


def _has_cyrillic(text: str) -> bool:
    return any(_is_cyrillic(ch) for ch in text)


def _has_latin(text: str) -> bool:
    return any(_is_latin(ch) for ch in text)

# --- Доп. нормализация только для local-part (левая часть до '@') ---
_LOCAL_HOMO_MAP = str.maketrans(
    {
        # кириллица → латиница (похоже выглядят)
        "а": "a",
        "е": "e",
        "о": "o",
        "р": "p",
        "с": "c",
        "х": "x",
        "у": "y",
        "к": "k",
        "м": "m",
        "т": "t",
        "н": "h",
        "в": "b",
        "і": "i",
        "А": "A",
        "Е": "E",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Х": "X",
        "У": "Y",
        "К": "K",
        "М": "M",
        "Т": "T",
        "Н": "H",
        "В": "B",
        "І": "I",
        # OCR: мягкий знак часто распознаётся вместо латинской 'b'
        "ь": "b",
        "Ь": "B",
    }
)
_DOT_VARIANTS = r"[\u00B7\u2022\u2219\u22C5\u2027\u30FB\u0387\u2024\u2044]"  # · • ∙ ⋅ ․ ・ · (one-dot leader) / (slash as dot в OCR)
# Кандидат local-part: допускаем ASCII-символы e-mail и кириллические гомоглифы,
#                      но границу слева проверяем только по ASCII-набору
_LOCAL_BOUNDARY = "A-Za-z0-9._%+-"
_LOCAL_BASE = "A-Za-z0-9._%+"
_LOCAL_HOMO = "аеорсхукмтнвіАЕОРСХУКМТНВІьЬ"
_LOCAL_DOTS = "\u00b7\u2022\u2219\u22c5\u2027\u30fb\u0387\u2024\u2044"
_LOCAL_CANDIDATE = re.compile(
    rf"(?<!\w)(?P<local>[{_LOCAL_BASE}{_LOCAL_HOMO}{_LOCAL_DOTS}-]{{1,64}})@(?P<rest>[^\s<>\[\]\(\)\{{\}}]+)"
)


def _normalize_localparts(text: str) -> str:
    """
    Исправляет только local-part (до '@'):
      - кириллические гомоглифы → латиница
      - псевдоточки (· • ∙ ․ ・) → '.'
    Домены не меняются.
    """

    def _fix(m: re.Match) -> str:
        local = m.group("local")
        # заменяем «псевдоточки» на обычную точку
        local = re.sub(_DOT_VARIANTS, ".", local)
        # приводим гомоглифы к латинице, если итог в ASCII
        translated = local.translate(_LOCAL_HOMO_MAP)
        if translated.isascii():
            local = translated
        rest = m.group("rest")
        if "." in rest:
            rest = re.sub(_DOT_VARIANTS, "", rest)
        else:
            rest = re.sub(_DOT_VARIANTS, ".", rest)
        return f"{local}@{rest}"

    try:
        return _LOCAL_CANDIDATE.sub(_fix, text)
    except Exception:
        return text


# Ядро адреса для lookahead (не использовать для замены самого адреса!)
# Допускаем любые непробельные символы в local-part и домене
_LOCAL_START_CHARS = "A-Za-z0-9А-Яа-яЁё"
_LOCAL_BODY_CHARS = _LOCAL_START_CHARS + "._%+\\-"
_EMAIL_CORE_ASCII = r"[A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
_EMAIL_CORE_UNICODE = (
    rf"[{_LOCAL_START_CHARS}][{_LOCAL_BODY_CHARS}]*@[A-Za-z0-9.-]+\.[A-Za-z]{{2,}}"
)
_EMAIL_CORE = _EMAIL_CORE_ASCII

_TLD_RE = r"(?:[A-Za-z]{2,24})"

_TRAILING_GARBAGE_CHARS = re.escape(">)]:;, }")

_TAIL_CUT = re.compile(
    rf"^(.+?\.(?:{_TLD_RE}))([{_TRAILING_GARBAGE_CHARS}].*)$"
)

_COMMON_TLDS = (
    "ru",
    "рф",
    "su",
    "com",
    "org",
    "net",
    "edu",
    "gov",
    "info",
    "biz",
    "kz",
    "by",
    "ua",
    "uz",
    "kg",
    "az",
    "am",
    "ge",
    "md",
    "tj",
    "tm",
    "pl",
    "cz",
    "de",
    "fr",
    "it",
    "es",
    "co",
    "io",
    "me",
    "us",
    "uk",
    "ca",
    "asia",
    "top",
    "site",
    "club",
    "online",
    "store",
    "tech",
)

_COMMON_TLD_RE = "|".join(
    sorted((re.escape(tld) for tld in _COMMON_TLDS), key=len, reverse=True)
)

EMAIL_RE_STRICT = re.compile(
    (
        r"""
    (?<![A-Za-z0-9._%+\-])                                  # слева не кусок e-mail
    (?![^@]*\.\.)                                            # без двойной точки в local-part
    """
        + rf"[{_LOCAL_START_CHARS}](?:[{_LOCAL_BODY_CHARS}]{{0,62}}[{_LOCAL_START_CHARS}])?"
        + r"""
    @
    (?:[\w](?:[\w\-]{0,61}[\w])?\.)+                     # доменные лейблы (ASCII/Unicode)
    [\w]{2,24}                                            # TLD
    (?!\w)                                                # справа НЕ буква/цифра/подчёркивание
"""
    ),
    re.VERBOSE,
)

def _trim_after_tld(addr: str) -> str:
    """
    Обрезает все хвосты после корректного TLD.
    Пример: 'ivan@mail.ru>:' -> 'ivan@mail.ru'
    """

    m = _TAIL_CUT.match(addr)
    if m:
        return m.group(1)
    m = _CAMELTAIL_RE.match(addr)
    if m:
        return m.group(1)
    return addr


_CAMELTAIL_RE = re.compile(
    rf"^(.+?\.(?:{_TLD_RE}))([A-Z][a-z]+(?:[A-Z][a-z]+)*)$"
)


def _strip_footnotes_before_email(addr: str) -> str:
    """
    Убирает сноски вида (a), [1] перед адресом.
    Не трогает первую букву local-part.
    """

    return re.sub(
        r"(?<!\w)\s*(?:\[(?:\d+|[a-z]{1,2})\]|\((?:\d+|[a-z]{1,2})\))\s*(?=[A-Za-z0-9._%+\-]{1,64}@)",
        "",
        addr,
        flags=re.IGNORECASE,
    )


_ALNUM = set(string.ascii_letters + string.digits)


# === EB-GENERIC-GLUE-SUSPECTS helpers (без словаря токенов) ===
def _starts_with_long_digits(local: str, n: int = 5) -> bool:
    """Локал начинается с >= n цифр подряд."""
    if not local:
        return False
    run = 0
    for ch in local:
        if ch.isdigit():
            run += 1
            if run >= n:
                return True
        else:
            break
    return False


_ORCID_PREFIX_RE = re.compile(r"^(?:\d{4}-){3,}\d{3,}[-\d]*", re.ASCII)


def _starts_with_orcid_like(local: str) -> bool:
    """Локал начинается с ORCID-подобной цифровой схемы (####-####-####-...)."""
    return bool(_ORCID_PREFIX_RE.match(local or ""))


def _long_alpha_run_no_separators(local: str, min_len: int = 14) -> bool:
    """
    Очень длинная буквеная «простыня» без точек/подчёркиваний/плюсов/цифр.
    Универсальный индикатор склейки слов слева (без словарей).
    """
    if not local or len(local) < min_len:
        return False
    if not all(ch.isalpha() for ch in local):
        return False
    if any(ch in "._+-" for ch in local):
        return False
    return True


def _prev_is_glued_letter(text: str, start: int) -> bool:
    """
    Перед адресом в исходном тексте стоит буква без разделителя
    (вероятная «склейка» предыдущего слова с локалом).
    """
    if start <= 0 or not text:
        return False
    prev = text[:start]
    if not prev:
        return False
    idx = len(prev) - 1
    while idx >= 0 and prev[idx].isspace():
        idx -= 1
    if idx < 0:
        return False
    punct_tail = ".,;:!?)]}»›\"'“”«…-–—"
    while idx >= 0 and prev[idx] in punct_tail:
        idx -= 1
    if idx < 0:
        return False
    return prev[idx].isalpha()



def _fix_hyphen_breaks(s: str) -> str:
    """
    Чиним переносы с дефисом: '-\n' внутри адресов оставляем как дефис,
    обычные переносы '-\n' вне адресов — убираем.
    Эвристика: если слева [A-Za-z0-9] и справа [A-Za-z0-9@], сохраняем '-'.
    """
    if not s:
        return s
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    def repl(m: re.Match) -> str:
        i = m.start()
        left = s[i - 1 : i]
        right = s[m.end() : m.end() + 1]
        if (
            left
            and right
            and re.match(r"[A-Za-z0-9]", left)
            and re.match(r"[A-Za-z0-9@]", right)
        ):
            return "-"  # «реальный» дефис (в т.ч. перед @ в логине)
        return ""  # мягкий перенос

    return re.sub(r"-(?:\s*\n\s*)", repl, s)


def _fix_hyphenation(text: str) -> str:
    """Склеиваем слова, разорванные переносом: a-\nndrew → andrew"""
    return re.sub(r"([A-Za-z0-9])-\n([A-Za-z0-9])", r"\1\2", text)


def _ensure_space_before_emails(s: str) -> str:
    """Добавляет пробел перед адресом, если он слипся с предыдущим словом."""

    if not s:
        return s

    out: list[str] = []
    last = 0
    for m in re.finditer(_EMAIL_CORE, s):
        start, _ = m.span()
        segment = s[last:start]
        if start > 0 and not s[start - 1].isspace():
            out.append(segment + " ")
        else:
            out.append(segment)
        last = start
    out.append(s[last:])
    return "".join(out)


def _ensure_space_after_emails(s: str) -> str:
    """Добавляет пробел после адреса, если сразу идёт буква или цифра."""

    if not s:
        return s

    out: list[str] = []
    last = 0
    for m in re.finditer(_EMAIL_CORE, s):
        start, end = m.span()
        out.append(s[last:start])
        out.append(s[start:end])
        if end < len(s) and not s[end].isspace() and s[end].isalnum():
            out.append(" ")
        last = end
    out.append(s[last:])
    return "".join(out)


def _strip_inline_footnotes(s: str) -> str:
    """
    Удаляем типовые сноски ([12], (a), надстрочные), НО только если
    после них НЕ начинается e-mail. Защищает первую букву a/b/c.
    """
    if not s:
        return s
    if _FOOTNOTES_MODE == "off":
        return s
    # допускаем необязательные пробелы перед адресом: (a)[пробелы]alex@...
    s = re.sub(
        rf"\s*(?:\[(?:\d+|[a-z]{{1,2}})\]|\((?:\d+|[a-z]{{1,2}})\))(?=\s*{_EMAIL_CORE})",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # иначе — это действительно сноска, удаляем её
    s = re.sub(
        r"(?<=\w)\s*(?:\[(?:\d+|[a-z]{1,2})\]|\((?:\d+|[a-z]{1,2})\))",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        rf"(?:[\u00B9\u00B2\u00B3\u2070-\u2079\u02B0-\u02B8\u2460-\u2473])\s*(?!{_EMAIL_CORE})",
        "",
        s,
    )
    return s


def _normalize_text(s: str, *, already_deobfuscated: bool = False) -> str:
    s = re.sub(
        r"[\u00B9\u00B2\u00B3\u2070-\u2079\u02B0-\u02B8\u2460-\u2473\u1D43-\u1D61\u1D62-\u1D6A]",
        "",
        s,
    )
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(_SUPERSCRIPT_MAP)
    s = s.translate(str.maketrans(_CIRCLED_MAP))
    # заменяем переносы строк, табы и NBSP на пробел
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _fix_hyphenation(s)
    s = s.replace("\n", " ").replace("\t", " ")
    s = s.replace("\xa0", " ")
    # 1) невидимые/биди/soft-hyphen
    s = strip_invisibles(s)
    # 2) склеить переносы с дефисом: 'a-\nndrew' → 'andrew'
    # (после удаления невидимых символов может появиться \n)
    s = _fix_hyphenation(s)
    # 3) починить дефис-переносы, чтобы не ломать 'shestova-ma@...'
    s = _fix_hyphen_breaks(s)
    if OBFUSCATION_ENABLE and not already_deobfuscated:
        s = deobfuscate_text(s)
    s = _normalize_localparts(s)
    s = re.sub(r"@([^,\s]+),([A-Za-z]{2,})\b", r"@\1.\2", s)
    # 6) разлипание границы перед адресом (не трогаем сам адрес)
    s = _ensure_space_before_emails(s)
    # 7) разлипание границы после адреса, когда за ним сразу цифры/буквы
    s = _ensure_space_after_emails(s)
    # 8) «умные» сноски — после разлипаний и уже на «живом» окружении
    s = _strip_inline_footnotes(s)
    # сжимаем повторяющиеся пробелы
    s = re.sub(r" {2,}", " ", s)
    return s


# Универсальная юникод-граница:
#  - слева: адрес НЕ может начинаться внутри слова → запрет на латинско-цифровой символ и '@'
#           (все разделители — точка, запятая, двоеточие, кавычки, скобки, тире и т.п. — допустимы)
#  - справа: адрес НЕ продолжается буквенно-цифровым, точкой или дефисом (не «врастать» в слово/доменные хвосты)
_EMAIL_CORE_RE = re.compile(
    rf"(?<![A-Za-z0-9_@])"
    rf"([{_LOCAL_START_CHARS}][{_LOCAL_BODY_CHARS}]*)"
    r"@"
    r"([\w.-]+\.[\w]{2,})"  # домен (разрешаем Unicode)
    r"(?![\w.-])",
    re.IGNORECASE,
)

_ASCII_LOCAL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+$")
_ASCII_DOMAIN_RE = re.compile(
    r"^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"(?:[A-Za-z]{2,24}|xn--[A-Za-z0-9-]{2,59})$"
)

ROLE_PREFIX_BLACKLIST = re.compile(
    r"^(russia|россия|journal|editor|info|ojs|office|support|contact|press|admissions|department|kafedra|кафедр|faculty|факультет)",
    re.IGNORECASE,
)
_ROLE_PREFIX_ALWAYS = (
    "russia",
    "россия",
    "journal",
    "editor",
    "info",
    "support",
)
GLUE_RISK_CONTEXT = re.compile(
    r"(fig\.?|рис\.?|табл\.?|doi|страна|country|\b[0-9]{2,}\b)",
    re.IGNORECASE,
)

_LOCAL_TLD_GLUE_CAMEL_RE = re.compile(rf"\.(?i:({_COMMON_TLD_RE}))(?=[A-Z0-9А-ЯЁ])")
_LOCAL_TLD_GLUE_VOWEL_RE = re.compile(
    r"\.(?:ru|su|ua|by|kz|kg|uz|tj|tm|az|am|ge|md)(?=[AEIOUYaeiouyАЕЁИОУЫЭЮЯаеёиоуыэюя])"
)
_EMAIL_FULL_RE = re.compile(rf"^{_EMAIL_CORE_UNICODE}$", re.IGNORECASE)

def extract_emails(text: str) -> list[str]:
    """
    Аккуратный экстрактор: достаёт «чистое ядро» e-mail без внешней пунктуации.
    """
    t = _normalize_text(text)
    out = []
    for m in _EMAIL_CORE_RE.finditer(t):
        local, domain = m.group(1), m.group(2)
        email = f"{local}@{domain}"
        email = _PUNCT_TRIM_RE.sub("", email)
        # частые артефакты после домена (обрывки URL-параметров)
        email = re.sub(r"(\?|\#|/).*$", "", email)
        out.append(email.lower())
    return out


# -------- Канонизация для дедупликации (не для отправки!) --------
_GMAIL_DOMAINS = {"gmail.com", "googlemail.com"}
_YANDEX_DOMAINS = {"yandex.ru", "ya.ru", "yandex.com"}
_MAILRU_DOMAINS = {"mail.ru", "bk.ru", "inbox.ru", "list.ru", "internet.ru"}


def _strip_plus_tag(local: str) -> str:
    i = local.find("+")
    return local[:i] if i != -1 else local


def canonicalize_email(addr: str) -> str:
    """
    Каноническая форма адреса для сравнения/дедупликации.
    - gmail/googlemail: убрать точки в local-part и '+tag'
    - yandex/mail.ru-семейство: убрать только '+tag'
    - всё остальное: только lower-case
    NB: Возвращаем канон для сравнения, НО в отправку всегда идёт исходный адрес.
    """

    try:
        a = addr.strip()
        if "@" not in a:
            return a.lower()
        local, domain = a.split("@", 1)
        d = domain.strip().lower()
        local_norm = local.strip()
        if d in _GMAIL_DOMAINS:
            local_norm = local_norm.replace(".", "")
            local_norm = _strip_plus_tag(local_norm)
        elif d in _YANDEX_DOMAINS or d in _MAILRU_DOMAINS:
            local_norm = _strip_plus_tag(local_norm)
        return f"{local_norm.lower()}@{d}"
    except Exception:
        return addr.lower()


def dedupe_keep_original(emails: list[str]) -> list[str]:
    """Удаляет дубликаты по канонической форме, сохраняя первый исходный адрес."""

    seen: set[str] = set()
    out: list[str] = []
    for e in emails:
        key = canonicalize_email(e)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def drop_leading_char_twins(emails: list[str]) -> list[str]:
    """Удаляет адреса, чья локальная часть равна локальной части другого адреса
    без первого символа, при условии одинакового домена. Сохраняем более длинный
    адрес, «срезанный» отбрасываем. Сравнение регистронезависимое для домена,
    регистрозависимое для локала."""

    if len(emails) <= 1:
        return emails

    groups: dict[str, dict[str, str]] = {}
    for e in emails:
        try:
            local, domain = e.split("@", 1)
        except ValueError:
            continue
        groups.setdefault(domain.lower(), {})[local] = e

    to_drop: set[tuple[str, str]] = set()
    log_pairs: list[tuple[str, str]] = []
    for domain, mapping in groups.items():
        locals_set = set(mapping.keys())
        for local in locals_set:
            if len(local) < 2:
                continue
            trimmed = local[1:]
            if trimmed in locals_set:
                to_drop.add((domain, trimmed))
                log_pairs.append((mapping[local], mapping[trimmed]))

    out: list[str] = []
    for e in emails:
        try:
            local, domain = e.split("@", 1)
        except ValueError:
            out.append(e)
            continue
        if (domain.lower(), local) not in to_drop:
            out.append(e)

    if os.getenv("DEBUG_EMAIL_PARSE_LOG") == "1" and log_pairs:
        try:
            log_path = _debug_log_path()
            ensure_parent(log_path)
            with log_path.open("a", encoding="utf-8") as f:
                for kept, dropped in log_pairs:
                    f.write(f"DROP_TRIMMED_TWIN: kept={kept} dropped={dropped}\n")
        except Exception:
            pass

    return out


# === Unified pipeline =========================================================


def _debug_enabled() -> bool:
    return os.getenv("EMAIL_PARSE_DEBUG", "0") == "1"


def _dbg(step: str, payload: list | str, limit: int = 5) -> None:
    if not _debug_enabled():
        return
    try:
        if isinstance(payload, list):
            show = payload[:limit]
            logger.debug(
                "[parse] %s: %s%s",
                step,
                show,
                " …" if len(payload) > limit else "",
            )
        else:
            snippet = (payload[:300] + "…") if len(payload) > 300 else payload
            logger.debug("[parse] %s: %s", step, snippet)
    except Exception:
        pass


def _is_ascii_local(local: str) -> bool:
    return bool(_ASCII_LOCAL_RE.fullmatch(local))


def _is_bad_prefix(local: str) -> bool:
    if not local:
        return False
    lowered = local.lower()
    if any(lowered.startswith(prefix) for prefix in _ROLE_PREFIX_ALWAYS):
        return True
    m = ROLE_PREFIX_BLACKLIST.match(local)
    if not m:
        return False
    rest = local[m.end() :]
    if not rest:
        return False
    return rest[0].isdigit()


def _looks_glued_around(text: str, start: int, end: int) -> bool:
    """
    Проверяем контекст вокруг найденного адреса: если слева "липкое" слово
    (таблица/рисунок/doi/страна/числа) и адрес прилип без разделителя,
    считаем, что это склейка и лучше отбросить.
    """

    if not text or start <= 0:
        return False
    left = text[start - 1]
    if not left.isalnum():
        return False
    L = max(0, start - 24)
    R = min(len(text), end + 24)
    ctx = text[L:R]
    return bool(GLUE_RISK_CONTEXT.search(ctx))


def parse_emails_unified(text: str, return_meta: bool = False):
    """Единый вход парсинга с учётом фичефлагов."""

    raw = text or ""
    _dbg("raw", raw)

    deobf_rules: list[str] = []
    deobf_applied = False
    should_deobf = OBFUSCATION_ENABLE or bool(_AUTO_DEOBF_HINTS.search(raw))
    if should_deobf:
        t1 = deobfuscate_text(raw)
        deobf_applied = t1 != raw
        if hasattr(deobfuscate_text, "last_rules"):
            try:
                deobf_rules = list(getattr(deobfuscate_text, "last_rules"))
            except Exception:
                deobf_rules = []
    else:
        t1 = raw
    _dbg("deobfuscated", t1)

    t2 = _normalize_text(t1, already_deobfuscated=deobf_applied)
    _dbg("normalized", t2)
    matches = list(EMAIL_RE_STRICT.finditer(t2))
    found = [m.group(0) for m in matches]
    _dbg("found", found)

    DEBUG_PARSE = os.getenv("DEBUG_EMAIL_PARSE", "0") == "1"
    DEBUG_LOG = os.getenv("DEBUG_EMAIL_PARSE_LOG", "1") == "1"
    log_path: Path | None = None
    if DEBUG_PARSE and DEBUG_LOG:
        try:
            log_path = _debug_log_path()
            ensure_parent(log_path)
        except Exception:
            log_path = None

    cleaned: list[str] = []
    final_reasons: list[str | None] = []
    items_meta: list[dict[str, object]] = []
    confusables_fixed = 0

    for m in matches:
        c = m.group(0)
        try:
            raw_local, raw_domain = c.split("@", 1)
        except ValueError:
            continue

        norm_local, norm_domain = raw_local, raw_domain
        conf_fixed = False
        if CONFUSABLES_NORMALIZE:
            norm_local, norm_domain, conf_fixed = normalize_confusables(raw_local, raw_domain)

        candidate = f"{norm_local}@{norm_domain}"
        start, end = m.span(0)
        sanitized, sanitize_reason = sanitize_email(candidate)
        reverted = False

        if not sanitized and sanitize_reason == "invalid-idna" and conf_fixed:
            sanitized, sanitize_reason = sanitize_email(c)
            reverted = sanitized != ""
            if reverted:
                conf_fixed = False

        finalized, finalize_reason, finalize_stage = finalize_email(
            norm_local,
            norm_domain,
            raw_text=t2,
            span=(start, end),
            sanitized=sanitized,
            sanitize_reason=sanitize_reason,
        )

        final_reason = sanitize_reason
        stage = None
        sanitized_final = finalized if finalized else ""
        if finalize_reason:
            sanitized_final = ""
            final_reason = finalize_reason
            stage = finalize_stage
        elif sanitized_final:
            if final_reason:
                stage = "sanitize"
        else:
            stage = "sanitize" if final_reason else stage

        if sanitized_final:
            if conf_fixed:
                confusables_fixed += 1
                if final_reason is None:
                    final_reason = "confusables-normalized"
            elif final_reason is None and deobf_applied:
                if c not in raw and sanitized_final not in raw:
                    final_reason = "obfuscation-applied"

            cleaned.append(sanitized_final)
            final_reasons.append(final_reason)
        items_meta.append(
            {
                "raw": c,
                "normalized": candidate,
                "sanitized": sanitized_final,
                "reason": final_reason,
                "stage": stage,
                "confusables_applied": conf_fixed,
                "reverted": reverted,
                "span": (start, end),
            }
        )

        if DEBUG_PARSE:
            try:
                print(
                    "[EMAIL-PARSE] raw=%r -> sanitized=%r reason=%r"
                    % (c, sanitized, final_reason)
                )
            except Exception:
                pass
            if log_path is not None:
                try:
                    rec = {
                        "raw": c,
                        "sanitized": sanitized,
                        "reason": final_reason,
                        "ts": __import__("datetime").datetime.utcnow().isoformat() + "Z",
                    }
                    with log_path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                except Exception:
                    pass

    _dbg("sanitized", cleaned)
    if not return_meta:
        return cleaned

    # === EB-GENERIC-GLUE-SUSPECTS: формируем список "подозрительных" адресов ===
    suspects: list[str] = []
    try:
        def _candidate_from_item(item: dict[str, object]) -> str:
            raw_candidate = str(
                item.get("sanitized")
                or item.get("normalized")
                or item.get("raw")
                or ""
            ).strip()
            if not raw_candidate or "@" not in raw_candidate:
                return ""
            sanitized_candidate, _ = sanitize_email(raw_candidate)
            candidate = sanitized_candidate or raw_candidate
            return candidate.lower()

        for item in items_meta:
            candidate = _candidate_from_item(item)
            if not candidate or "@" not in candidate:
                continue
            loc, _dom = candidate.split("@", 1)
            if (
                _starts_with_long_digits(loc)
                or _starts_with_orcid_like(loc)
                or _long_alpha_run_no_separators(loc)
            ):
                suspects.append(candidate)

        for item in items_meta:
            span = item.get("span")
            start = None
            if isinstance(span, (list, tuple)) and len(span) == 2:
                try:
                    start = int(span[0])
                except Exception:
                    start = None
            if start is None:
                continue
            if not _prev_is_glued_letter(t2, start):
                continue
            candidate = _candidate_from_item(item)
            if candidate and "@" in candidate:
                suspects.append(candidate)
    except Exception:
        pass
    suspects = sorted(set(suspects))

    deobfuscated_count = sum(
        1 for r in deobf_rules if r and not r.startswith("#")
    )

    items = items_meta

    meta = {
        "items": items,
        "deobfuscated": deobf_applied,
        "deobfuscated_count": deobfuscated_count,
        "confusables_fixed": confusables_fixed,
        "deobfuscation_rules": deobf_rules,
        "suspects": suspects,  # EB-GENERIC-GLUE-SUSPECTS
    }
    return cleaned, meta


# Сноски: убираем ТОЛЬКО надстрочные цифры/буквы, не трогаем обычные латинские
# ¹²³⁰–⁹, ᵃ…  (диапазоны супертекстовых символов)
_SUPERSCRIPT_FOOTNOTE_RE = re.compile(
    r"^[\u00B9\u00B2\u00B3\u2070-\u2079\u1D43-\u1D61\u1D62-\u1D6A]+"
)


def _strip_footnotes(local: str) -> str:
    """Удаляет ведущие надстрочные символы-сноски из local-part."""
    # теперь удаляем только надстрочные «сноски»
    return _SUPERSCRIPT_FOOTNOTE_RE.sub("", local)


def _normalize_dots(local: str) -> str:
    s = local.strip(".")
    while ".." in s:
        s = s.replace("..", ".")
    return s


def normalize_confusables(local: str, domain: str) -> tuple[str, str, bool]:
    """Нормализует безопасные кириллические гомоглифы в local/domain."""

    email = f"{local}@{domain}"
    if not email or "@" not in email:
        return local, domain, False

    if not _has_cyrillic(email):
        return local, domain, False

    replaced = email.translate(_CONFUSABLE_TRANSLATION)
    if replaced == email:
        return local, domain, False

    if not _has_latin(email):
        if not _ASCII_EMAIL_RE.fullmatch(replaced):
            return local, domain, False

    new_local, new_domain = replaced.split("@", 1)
    new_local = unicodedata.normalize("NFKC", new_local)
    new_domain = unicodedata.normalize("NFKC", new_domain)

    # Не допускаем «смешанных» результатов вроде "Иbah": если после замены
    # остались символы обоих алфавитов, откатываем изменения, иначе sanitize
    # забракует адрес как mixed-script-local.
    if _has_cyrillic(new_local) and _has_latin(new_local):
        new_local = local
    if _has_cyrillic(new_domain) and _has_latin(new_domain):
        new_domain = domain

    if new_local == local and new_domain == domain:
        return local, domain, False

    changed = new_local != local or new_domain != domain
    return new_local, new_domain, changed


def normalize_domain(domain: str) -> tuple[str, str | None]:
    """Преобразует домен к IDNA, проверяя ограничения RFC."""

    domain = (domain or "").strip().rstrip(".")
    if not domain:
        return "", "invalid-idna"

    domain = unicodedata.normalize("NFKC", domain)
    domain = domain.lower()

    labels = domain.split(".")
    ascii_labels: list[str] = []
    for label in labels:
        if not label:
            return "", "invalid-idna"
        label_nfkc = unicodedata.normalize("NFKC", label)
        try:
            label_nfkc.encode("ascii")
            ascii_label = label_nfkc
        except UnicodeEncodeError:
            try:
                ascii_label = idna.encode(label_nfkc, uts46=True).decode("ascii")
            except idna.IDNAError:
                return "", "invalid-idna"
        ascii_label = ascii_label.lower()
        if not (1 <= len(ascii_label) <= 63):
            return "", "invalid-idna"
        if ascii_label.startswith("-") or ascii_label.endswith("-"):
            return "", "invalid-idna"
        if not _ASCII_DOMAIN_LABEL_RE.fullmatch(ascii_label):
            return "", "invalid-idna"
        ascii_labels.append(ascii_label)

    ascii_domain = ".".join(ascii_labels)
    if len(ascii_domain) > 253:
        return "", "invalid-idna"

    tld = ascii_labels[-1]
    if not tld.startswith("xn--") and not (2 <= len(tld) <= 24 and tld.isalpha()):
        return "", "invalid-idna"

    if not _ASCII_DOMAIN_RE.match(ascii_domain):
        return "", "invalid-idna"

    return ascii_domain, None


def _preserve_leading_alnum(original: str, cleaned: str) -> str:
    try:
        o_loc, o_dom = original.split("@", 1)
        c_loc, c_dom = cleaned.split("@", 1)
    except ValueError:
        return cleaned
    if o_dom != c_dom or not o_loc or not c_loc:
        return cleaned
    if o_loc[0] in _ALNUM and c_loc[0] != o_loc[0]:
        if len(o_loc) > 1 and c_loc.startswith(o_loc[1:]):
            return original
    return cleaned


AGGR = os.getenv("AGGRESSIVE_LOCAL_REPAIR", "0") == "1"
_POPULAR = re.compile(r"^(?:yandex|ya|gmail|mail|bk|list|rambler|inbox)\.", re.I)
_REPAIR_RE = re.compile(r"^[a-z]{5,}\d+([a-z0-9._+\-]{4,})$", re.I)


def sanitize_email(email: str, strip_footnote: bool = True) -> tuple[str, str | None]:
    """Финальная чистка и проверка адреса."""

    email_original = email
    reason: str | None = None

    trimmed = _trim_after_tld(email)
    if trimmed != email:
        reason = reason or "trailing-garbage"
    email = trimmed

    email = _strip_footnotes_before_email(email)

    if strip_footnote and "@" in email:
        local0, domain0 = email.split("@", 1)
        email = f"{_strip_footnotes(local0)}@{domain0}"

    normalized_text = _normalize_text(email)
    compact_original = normalized_text.replace(" ", "").strip()
    s = compact_original.lower()
    before_trim = s
    s_trimmed = _PUNCT_TRIM_RE.sub("", s)
    compact_trimmed = _PUNCT_TRIM_RE.sub("", compact_original)
    if s_trimmed != before_trim and reason is None:
        reason = "punct-trimmed"
    trimmed_tail = re.sub(r"(\?|\#|/).*$", "", s_trimmed)
    compact_tail = re.sub(r"(\?|\#|/).*$", "", compact_trimmed)
    if trimmed_tail != s_trimmed and reason is None:
        reason = "trailing-garbage"
    s = trimmed_tail
    compact_original = compact_tail

    if "@" not in s:
        return "", reason

    local, domain = s.split("@", 1)
    original_local = ""
    if "@" in compact_original:
        original_local = compact_original.split("@", 1)[0]
    if not original_local:
        original_local = local
    before_local = local
    local = local.replace(",", ".")
    original_local = original_local.replace(",", ".")
    local = re.sub(r"^[-_.]+|[-_.]+$", "", local)
    original_local = re.sub(r"^[-_.]+|[-_.]+$", "", original_local)
    local = _normalize_dots(local)
    original_local = _normalize_dots(original_local)
    if local != before_local and reason is None:
        reason = "punct-trimmed"

    if _has_cyrillic(local) and _has_latin(local):
        return "", "mixed-script-local"

    if not _is_ascii_local(local):
        return "", "non-ascii-local"

    if _is_bad_prefix(local):
        return "", "role-like-prefix"

    if _LOCAL_TLD_GLUE_CAMEL_RE.search(original_local) or _LOCAL_TLD_GLUE_VOWEL_RE.search(
        original_local
    ):
        return "", "skleyka-in-local"

    domain_ascii, domain_reason = normalize_domain(domain)
    if not domain_ascii:
        return "", domain_reason or reason

    if not is_allowed_domain(domain_ascii):
        return "", "tld-not-allowed"

    if AGGR and _POPULAR.match(domain_ascii):
        m = _REPAIR_RE.match(local)
        if m:
            local = m.group(1)

    normalized = f"{local}@{domain_ascii}".lower()
    normalized = _preserve_leading_alnum(email_original, normalized)
    return normalized, reason


def finalize_email(
    local: str,
    domain: str,
    *,
    raw_text: str = "",
    span=None,
    sanitized: str | None = None,
    sanitize_reason: str | None = None,
) -> tuple[str, str, str]:
    """Finalize an e-mail candidate validating context-sensitive rules."""

    candidate = f"{local}@{domain}"
    sanitize_stage = "sanitize"

    start = end = None
    if span is not None:
        if isinstance(span, (list, tuple)) and len(span) == 2:
            try:
                start = int(span[0])
                end = int(span[1])
            except Exception:  # pragma: no cover - defensive conversion
                start = end = None
    if (
        raw_text
        and start is not None
        and end is not None
        and _looks_glued_around(raw_text, start, end)
    ):
        return "", "glued-break", "finalize"

    email = sanitized
    reason = sanitize_reason
    if email is None:
        email, reason = sanitize_email(candidate)

    if not email:
        return "", str(reason or "invalid"), sanitize_stage

    return email, "", "finalize"


def dedupe_with_variants(emails: list[str]) -> list[str]:
    """
    Дедуплицируем, учитывая пару (сноской)вариант → чистый вариант.
    Если есть и «¹alexandr…@» и «alexandr…@», оставляем чистый.
    """
    clean = [sanitize_email(e)[0] for e in emails]
    variants = [sanitize_email(e, strip_footnote=False)[0] for e in emails]

    pairs = [(c, v) for c, v in zip(clean, variants) if v]

    bucket: dict[str, set[str]] = {}
    for c, v in pairs:
        bucket.setdefault(c, set()).add(v)

    final = set()
    for key, vars_set in bucket.items():
        if key and key in vars_set:
            final.add(key)
            continue
        if key and len(vars_set) == 1:
            # only one variant – assume digits were footnotes and strip them
            final.add(key)
            continue

        clean_variants = {v for v in vars_set if _EMAIL_FULL_RE.fullmatch(v)}
        candidates = clean_variants or vars_set
        if not candidates:
            continue
        chosen = min(candidates, key=len)
        if chosen:
            final.add(chosen)

    # существующая логика... + провайдерная канонизация для сравнения
    def _canon(e: str) -> str:
        try:
            local, domain = e.split("@", 1)
        except ValueError:
            return e
        d = domain.lower()
        local_norm = local.lower()
        # Gmail: игнорируем точки, режем +tag
        if d in ("gmail.com", "googlemail.com"):
            local_norm = local_norm.split("+", 1)[0].replace(".", "")
        # Yandex: режем +tag
        if (
            d.endswith("yandex.ru")
            or d.endswith("yandex.com")
            or d.endswith("yandex.kz")
            or d.endswith("ya.ru")
        ):
            local_norm = local_norm.split("+", 1)[0]
        # Mail.ru: режем +tag
        if (
            d.endswith("mail.ru")
            or d.endswith("bk.ru")
            or d.endswith("inbox.ru")
            or d.endswith("list.ru")
        ):
            local_norm = local_norm.split("+", 1)[0]
        return f"{local_norm}@{d}"

    seen = {}
    out = []
    for e in sorted(final):
        key = _canon(e)
        if key not in seen:
            seen[key] = e
            out.append(e)
    return sorted(out)


def parse_manual_input(text: str) -> list[str]:
    """
    DEPRECATED shim: оставлено для обратной совместимости.
    Всегда вызывает parse_emails_unified(text).
    """
    return parse_emails_unified(text)


try:
    __all__
except NameError:  # pragma: no cover - module attribute guard
    __all__ = []  # type: ignore[var-annotated]

if "classify_email_role" not in __all__:
    __all__.append("classify_email_role")
if "finalize_email" not in __all__:
    __all__.append("finalize_email")
