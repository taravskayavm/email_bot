import logging
import os
import re
import unicodedata
from functools import lru_cache

import idna

logger = logging.getLogger(__name__)
_FOOTNOTES_MODE = (os.getenv("FOOTNOTES_MODE", "smart") or "smart").lower()

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
    r'^[\s\(\[\{<«"“”„‚’»>}\]\).,:;]+|[\s\(\[\{<«"“”„‚’»>}\]\).,:;]+$'
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
        # приводим гомоглифы к латинице
        local = local.translate(_LOCAL_HOMO_MAP)
        return f"{local}@{m.group('rest')}"

    try:
        return _LOCAL_CANDIDATE.sub(_fix, text)
    except Exception:
        return text


_OBF_AT = [
    r"\[at\]",
    r"\(at\)",
    r"\{at\}",
    r"\sat\s",
    r"\s@\s",
    r"\sat\s",
    r"\[собака\]",
    r"\(собака\)",
    r"\{собака\}",
    r"\sсобака\s",
]
_OBF_DOT = [
    r"\[dot\]",
    r"\(dot\)",
    r"\{dot\}",
    r"\sdot\s",
    r"\[точка\]",
    r"\(точка\)",
    r"\{точка\}",
    r"\sточка\s",
]


@lru_cache(maxsize=256)
def _deobfuscate(text: str) -> str:
    """
    Простейшая размаскировка: user [at] site [dot] ru → user@site.ru
    Поддерживает англ./рус. маркеры и произвольные пробелы/скобки.
    """
    t = text
    # унификация пробелов
    t = re.sub(r"\s+", " ", t)
    # замены at
    for pat in _OBF_AT:
        t = re.sub(pat, " @ ", t, flags=re.IGNORECASE)
    # замены dot
    for pat in _OBF_DOT:
        t = re.sub(pat, " . ", t, flags=re.IGNORECASE)
    # сжать пробелы и убрать их вокруг разделителей
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s*@\s*", "@", t)
    t = re.sub(r"\s*\.\s*", ".", t)
    # частые OCR-ошибки: запятая перед TLD
    t = re.sub(r"@([^,\s]+),([A-Za-z]{2,})\b", r"@\1.\2", t)
    return t


# Ядро адреса для lookahead (не использовать для замены самого адреса!)
# Допускаем любые непробельные символы в local-part и домене
_EMAIL_CORE = r"[A-Za-z0-9][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"


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
        if left and right and re.match(r"[A-Za-z0-9]", left) and re.match(r"[A-Za-z0-9@]", right):
            return "-"  # «реальный» дефис (в т.ч. перед @ в логине)
        return ""  # мягкий перенос

    return re.sub(r"-(?:\s*\n\s*)", repl, s)


def _fix_glued_boundaries(s: str) -> str:
    """
    Вставляем пробел между предшествующим символом и НАЧАЛОМ e-mail,
    если перед адресом нет пробела. Первая буква адреса НЕ трогается.
    Примеры: 'Россия.duslem@mail.ru' → 'Россия. duslem@mail.ru'
             'см:ivan@mail.ru'      → 'см: ivan@mail.ru'
             '—alex@mail.ru'        → '— alex@mail.ru'
    """
    if not s:
        return s
    # Вставляем пробел ПЕРЕД адресом, не потребляя ни одного символа адреса.
    # Слева допускаем любой не-пробельный (включая букву), чтобы разлепить 'словоivan@mail.ru'.
    # Но НЕ трогаем случаи, где слева уже пробел.
    out = []
    last = 0
    for m in re.finditer(_EMAIL_CORE, s):
        start, end = m.span()
        if start > 0 and s[start - 1] != " ":
            out.append(s[last:start] + " ")
        else:
            out.append(s[last:start])
        last = start
    out.append(s[last:])
    return "".join(out)


def _strip_footnotes(s: str) -> str:
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
        rf"(?<=\w)\s*(?:\[(?:\d+|[a-z]{{1,2}})\]|\((?:\d+|[a-z]{{1,2}})\))(?=\s*{_EMAIL_CORE})",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # иначе — это действительно сноска, удаляем её
    s = re.sub(
        rf"(?<=\w)\s*(?:\[(?:\d+|[a-z]{{1,2}})\]|\((?:\d+|[a-z]{{1,2}})\))",
        " ",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        rf"(?:[\u00B9\u00B2\u00B3\u2070-\u2079\u02B0-\u02B8])\s*(?!{_EMAIL_CORE})",
        "",
        s,
    )
    return s


def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(_SUPERSCRIPT_MAP)
    s = s.translate(str.maketrans(_CIRCLED_MAP))
    # заменяем переносы строк, табы и NBSP на пробел
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = s.replace("\n", " ").replace("\t", " ")
    s = s.replace("\xa0", " ")
    # 1) невидимые/биди/soft-hyphen
    s = strip_invisibles(s)
    # 2) починить дефис-переносы, чтобы не ломать 'shestova-ma@...'
    s = _fix_hyphen_breaks(s)
    # 3) размаскировка "at/dot/собака/точка" перед границами
    s = _deobfuscate(s)
    # 4) нормализация local-part (замены юникод-lookalike и т.п.)
    s = _normalize_localparts(s)
    # 5) разлипание границы перед адресом (не трогаем сам адрес)
    s = _fix_glued_boundaries(s)
    # 6) «умные» сноски — после разлипаний и уже на «живом» окружении
    s = _strip_footnotes(s)
    # сжимаем повторяющиеся пробелы
    s = re.sub(r" {2,}", " ", s)
    return s


# Универсальная юникод-граница:
#  - слева: адрес НЕ может начинаться внутри слова → запрет на латинско-цифровой символ и '@'
#           (все разделители — точка, запятая, двоеточие, кавычки, скобки, тире и т.п. — допустимы)
#  - справа: адрес НЕ продолжается буквенно-цифровым, точкой или дефисом (не «врастать» в слово/доменные хвосты)
_EMAIL_CORE_RE = re.compile(
    r"(?<![A-Za-z0-9_@])"
    r"([A-Za-z0-9][A-Za-z0-9._%+-]*)"
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

_TLD_PREFIXES = (
    "ru",
    "com",
    "net",
    "org",
    "gov",
    "edu",
    "info",
    "biz",
    "su",
    "ua",
    "рф",
)


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


def parse_emails_unified(text: str, return_meta: bool = False):
    """
    Единый вход парсинга:
      raw -> deobfuscate -> normalize -> find -> sanitize
    Возвращает список валидных адресов в порядке появления (дубли не убираются).
    При `return_meta=True` дополнительно возвращает словарь с метаданными.
    """

    raw = text or ""
    _dbg("raw", raw)
    t1 = _deobfuscate(raw)
    _dbg("deobfuscated", t1)
    t2 = _normalize_text(t1)
    _dbg("normalized", t2)
    matches = list(_EMAIL_CORE_RE.finditer(t2))
    found = [m.group(0) for m in matches]
    _dbg("found", found)
    cleaned = [e for e in (sanitize_email(x) for x in found) if e]
    _dbg("sanitized", cleaned)
    if not return_meta:
        return cleaned

    suspects: list[str] = []
    for m in matches:
        start = m.start(0)
        if start > 0:
            prev = t2[:start].rstrip()[-1:]
            first = m.group(0)[:1].lower()
            if prev in ".,:;()[]{}-—" and first in "abc":
                suspect = sanitize_email(m.group(0))
                if suspect:
                    suspects.append(suspect)

    return cleaned, {"suspects": suspects}


_LEADING_FOOTNOTE_RE = re.compile(
    r"^(?:\d{1,3})+(?=[A-Za-z])"
)  # 1–3 цифры в начале local-part


def _strip_leading_footnote(local: str) -> str:
    return _LEADING_FOOTNOTE_RE.sub("", local)


def sanitize_email(email: str, strip_footnote: bool = True) -> str:
    """
    Финальная чистка: убираем внешнюю пунктуацию, невидимые символы,
    откусываем ведущие цифры-сноски в local-part, обрезаем крайние -_. от переносов.
    """
    s = _normalize_text(email).lower().replace(" ", "").strip()
    s = _PUNCT_TRIM_RE.sub("", s)
    s = re.sub(r"(\?|\#|/).*$", "", s)

    if "@" not in s:
        return ""

    local, domain = s.split("@", 1)
    local = local.replace(",", ".")  # ошибки OCR: запятая вместо точки
    # убираем ведущие цифры-сноски, если требуется
    if strip_footnote:
        local = _strip_leading_footnote(local)
    # чистим края от .-_ оставшихся от переносов
    local = re.sub(r"^[-_.]+|[-_.]+$", "", local)

    # жёстко: local-part строго ASCII
    if not _ASCII_LOCAL_RE.match(local):
        return ""

    # If local part accidentally contains something that looks like a
    # domain with a known top-level domain followed by additional
    # characters (e.g. ``mail.ruovalov``), it is likely the result of two
    # concatenated addresses and should be rejected.
    for tld in _TLD_PREFIXES:
        # detect e.g. ``mail.ruovalov`` where ``.ru`` is followed by more letters
        if re.search(rf"\.{tld}[A-Za-z]", local):
            return ""

    # домен: приводим к IDNA (punycode), но запрещаем мусор
    domain = domain.rstrip(".")
    if not _ASCII_DOMAIN_RE.match(domain):
        # ВАЖНО: не трогаем Unicode-домен, кодируем через IDNA UTS#46
        try:
            domain = idna.encode(domain, uts46=True).decode("ascii")
        except Exception:
            return ""
    # Повторная проверка уже в ASCII
    if not _ASCII_DOMAIN_RE.match(domain):
        return ""

    return f"{local}@{domain}"


def dedupe_with_variants(emails: list[str]) -> list[str]:
    """
    Дедуплицируем, учитывая пару (сноской)вариант → чистый вариант.
    Если есть и «55alexandr…@» и «alexandr…@», оставляем чистый.
    """
    clean = [sanitize_email(e) for e in emails]
    variants = [sanitize_email(e, strip_footnote=False) for e in emails]

    pairs = [(c, v) for c, v in zip(clean, variants) if v]

    bucket: dict[str, set[str]] = {}
    for c, v in pairs:
        bucket.setdefault(c, set()).add(v)

    final = set()
    for key, vars_set in bucket.items():
        if key in vars_set:
            final.add(key)
        elif len(vars_set) == 1:
            # only one variant – assume digits were footnotes and strip them
            final.add(key)
        else:
            # multiple variants without a clean version: keep the shortest variant
            final.add(sorted(vars_set, key=len)[0])

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
