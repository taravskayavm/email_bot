from __future__ import annotations

import logging
import os
import re
import unicodedata
from typing import Iterable, Set

try:
    import idna  # type: ignore
except Exception:  # fallback
    idna = None  # type: ignore

logger = logging.getLogger(__name__)

CONFUSABLES_NORMALIZE = os.getenv("CONFUSABLES_NORMALIZE", "1") == "1"
OBFUSCATION_ENABLE = os.getenv("OBFUSCATION_ENABLE", "1") == "1"

# Наиболее частые кириллические гомоглифы → латиница (критично: «х»→'x')
CYR_TO_LAT = {
    "а": "a",
    "е": "e",
    "о": "o",
    "р": "p",
    "с": "c",
    "ь": "b",
    "л": "l",
    "у": "y",
    "к": "k",
    "х": "x",
    "в": "v",
    "м": "m",
    "т": "t",
    "н": "h",
    "А": "A",
    "В": "B",
    "Е": "E",
    "К": "K",
    "М": "M",
    "Н": "H",
    "О": "O",
    "Р": "P",
    "С": "C",
    "Т": "T",
    "Х": "X",
}

_INVISIBLES_RE = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2066-\u2069\uFEFF]")

EMAIL_RE = re.compile(r"(?ix)\b[a-z0-9._%+\-]+@(?:[a-z0-9\-]+\.)+[a-z0-9\-]{2,}\b")

# Варианты обфусцированных адресов до окончательной нормализации
_DOT_LIKE_CHARS = "·•∙‧⸳・"
_DOT_LIKE_CLASS = re.escape(_DOT_LIKE_CHARS)

OBFUSCATED_EMAIL_RE = re.compile(
    rf"""(?ix)
    \b
    ([a-z0-9][a-z0-9 \._\-\+\u0400-\u04FF{_DOT_LIKE_CLASS}]{{0,64}}?)
    (
        @
        |\(\s*at\s*\)
        |\[\s*at\s*\]
        |\{{\s*at\s*\}}
        |<\s*at\s*>
        |\b(?:at|собака)\b
        |&commat;
    )
    ([a-z0-9 \-\.\u0400-\u04FF{_DOT_LIKE_CLASS}\(\)\[\]\{{\}}<>]{{1,255}})
    \b
    """
)

# ВАЖНО: (?<!@) — чтобы доменная часть e-mail не считалась ссылкой
SAFE_URL_RE = re.compile(
    r"(?ix)(?<!@)\b((?:https?://)?(?:www\.)?[^\s<>()]+?\.[^\s<>()]{2,}[^\s<>()]*)(?=$|[\s,;:!?)}\]])"
)

# Символы, визуально похожие на точки (серединные/маркировочные)
_DOT_LIKE_RE = re.compile(rf"[{_DOT_LIKE_CLASS}]")

_AT_WORD_RE = re.compile(r"(?i)\b(?:at|собака)\b")
_DOT_WORD_RE = re.compile(r"(?i)\b(?:dot|точка)\b")
_AT_BRACKET_RE = re.compile(r"(?i)[\(\[\{<]\s*(?:at|собака)\s*[\)\]\}>]")
_DOT_BRACKET_RE = re.compile(r"(?i)[\(\[\{<]\s*(?:dot|точка)\s*[\)\]\}>]")

# ---------------------------------------------------------------------------
#  Дедупликация с сохранением ОРИГИНАЛА (нужна для emailbot/handlers/preview.py)
#  Объявляем РАНО (выше по файлу), чтобы точно успеть к моменту импорта.
# ---------------------------------------------------------------------------
def dedupe_keep_original(emails, return_map: bool = False):
    """
    Дедупликация по канону, но вернуть ПЕРВЫЙ встреченный ОРИГИНАЛ строки.
    Возвращает:
      - список оригиналов (в порядке первого появления) без дублей по канону;
      - при return_map=True → (result, canonical->set(originals)).
    """

    if not emails:
        return ([], {}) if return_map else []
    seen = set()
    mapping = {}
    result = []
    for raw in emails:
        e = (raw or "").strip()
        if not e:
            continue
        try:
            # canonical_email определена ниже в этом модуле; если порядок импорта
            # ещё не дошёл — используем мягкий фолбэк на lowercase.
            canon = canonical_email(e)  # type: ignore[name-defined]
        except Exception:
            canon = e.lower()
        mapping.setdefault(canon, set()).add(e)
        if canon in seen:
            continue
        seen.add(canon)
        result.append(e)
    if return_map:
        return result, mapping
    return result

# «Мусор» на краях токена: пробелы, NBSP/soft hyphen, пунктуация, кавычки, тире, маркеры списков
_LEADING_JUNK_RE = re.compile(
    r'^[\s\u00A0\u00AD\.\-–—·•_*~=:;|/\\<>\(\)\[\]\{\}"\'`«»„“”‚‘’]+'
)
_TRAILING_JUNK_RE = re.compile(
    r'[\s\u00A0\u00AD\.\-–—·•_*~=:;|/\\<>\(\)\[\]\{\}"\'`«»„“”‚‘’]+$'
)


def drop_leading_char_twins(s):
    """
    Legacy helper: убрать «здвоенные»/повторяющиеся ведущие символы и общую пунктуацию
    в начале токена (буллеты, тире, точки, кавычки и т.п.).
    """

    if isinstance(s, (list, tuple)):
        return type(s)(drop_leading_char_twins(item) for item in s)
    if not s:
        return s
    return _LEADING_JUNK_RE.sub("", s)


def drop_trailing_char_twins(s):
    """
    Парная функция: убрать хвостовой «мусор»/повторы пунктуации в конце токена.
    Добавлена на случай старых импортов в пайплайне.
    """

    if isinstance(s, (list, tuple)):
        return type(s)(drop_trailing_char_twins(item) for item in s)
    if not s:
        return s
    return _TRAILING_JUNK_RE.sub("", s)


def _normalize_confusables(text: str) -> str:
    if not text or not CONFUSABLES_NORMALIZE:
        return text
    t = unicodedata.normalize("NFC", text)
    return "".join(CYR_TO_LAT.get(ch, ch) for ch in t)


def strip_invisibles(text: str) -> str:
    return _INVISIBLES_RE.sub("", text or "").replace("\xad", "").replace("\u2060", "")


def _idna_domain(domain: str) -> str:
    d = domain.strip().rstrip(".").lower()
    if not d or idna is None:
        return d or domain
    try:
        return idna.encode(d, uts46=True).decode("ascii")
    except Exception:
        return d


def preclean_for_email_extraction(text: str) -> str:
    t = strip_invisibles(text or "")
    # убрать NBSP и мягкие переносы, выровнять пробелы
    t = t.replace("\u00A0", " ").replace("\u00AD", "")
    t = t.replace("\r", "\n")
    # Убираем разрывы вокруг @ и «точек»
    t = re.sub(r"\s*@\s*", "@", t)
    t = re.sub(rf"(?<=\w)[{_DOT_LIKE_CLASS}](?=\w)", ".", t)
    t = re.sub(r"[ \t]+", " ", t)
    return t.strip()


def _deobfuscate_chunks(chunks: Iterable[str]) -> Iterable[str]:
    if not OBFUSCATION_ENABLE:
        yield from chunks
        return
    subs = [
        (
            re.compile(
                r"(?i)\s*(?:\[at\]|\(at\)|\{at\}|<\s*at\s*>|\(a\)|&commat;| at | собака )\s*"
            ),
            "@",
        ),
        (
            re.compile(
                rf"(?i)\s*(?:\[dot\]|\(dot\)|\{{dot\}}|<\s*dot\s*>| dot | точка |[{_DOT_LIKE_CLASS}])\s*"
            ),
            ".",
        ),
    ]
    for s in chunks:
        t = s
        for rx, rep in subs:
            t = rx.sub(rep, t)
        needs_collapse = (
            "@" in t
            or _AT_WORD_RE.search(t)
            or _DOT_WORD_RE.search(t)
            or any(ch in t for ch in _DOT_LIKE_CHARS)
            or "&commat;" in t
        )
        if needs_collapse and " " not in t:
            t = _collapse_spaced_tokens(t)
        else:
            t = _DOT_LIKE_RE.sub(".", t)
        yield t


def _collapse_spaced_tokens(s: str) -> str:
    """Склеить последовательности вида ``i v a n @ m a i l . r u``."""

    if not s:
        return s

    t = _DOT_LIKE_RE.sub(".", s)
    t = _AT_BRACKET_RE.sub("@", t)
    t = _DOT_BRACKET_RE.sub(".", t)
    t = _AT_WORD_RE.sub("@", t)
    t = _DOT_WORD_RE.sub(".", t)
    t = t.replace("&commat;", "@")
    t = re.sub(r"\s*@\s*", "@", t)
    t = re.sub(r"\s*\.\s*", ".", t)
    t = re.sub(rf"\s*[{_DOT_LIKE_CLASS}]\s*", ".", t)

    # склейка одиночных символов, разделённых пробелами
    while True:
        new = re.sub(r"(?i)\b([a-z0-9])\s+(?=[a-z0-9@])", r"\1", t)
        if new == t:
            break
        t = new

    t = re.sub(r"\s+", "", t)
    return t


def preclean_obfuscations(text: str) -> str:
    """Return ``text`` normalised for matching while undoing simple obfuscations."""

    cleaned = preclean_for_email_extraction(text)
    if not cleaned:
        return ""
    from utils.email_deobfuscate import deobfuscate_text

    return deobfuscate_text(cleaned) if OBFUSCATION_ENABLE else cleaned


def parse_emails_unified(text: str, return_meta: bool = False):
    """
    Нормализация (гомоглифы/невидимые) → деобфускация → извлечение e-mail → IDNA для домена.
    """

    from utils.email_deobfuscate import deobfuscate_text

    src = preclean_for_email_extraction(text)
    src = re.sub(r"\{\d+\}(?=[A-Za-z])", " ", src)
    if OBFUSCATION_ENABLE:
        src = deobfuscate_text(src)
    # Repair only a missing domain separator, never join adjacent addresses.
    src = re.sub(r"(@[\w-]+),\s*(ru|com)\b", r"\1.\2", src, flags=re.I)
    src = re.sub(r"(?<=\w)\s*\.\s*(?=\w)", ".", src)
    token_re = re.compile(r"[A-Za-z0-9\u0400-\u04ff.!#$%&'*+/=?^_`{|}~-]+@(?:[^\W_]|[-.])+", re.UNICODE)
    found, items, suspects = [], [], []
    for match in token_re.finditer(src):
        raw = match.group(0).rstrip(".")
        local, domain = raw.split("@", 1)
        prefix = re.match(r"^[\u0400-\u04ff][\u0400-\u04ff.-]*[\u0400-\u04ff][.-]*", local)
        if prefix:
            local = local[prefix.end():].lstrip(".-")
        local = local.lstrip(".-")
        # Numeric markers are removed only with nearby explicit footnote evidence.
        if re.search(r"\[\d+\]|\(\d+\)", src):
            local = re.sub(r"^\d+(?=[A-Za-z])", "", local)
        original_domain = domain
        local, domain, changed = normalize_confusables(local, domain)
        reverted = False
        normalized_domain = normalize_domain(domain)
        if isinstance(normalized_domain, tuple):
            normalized_domain, domain_reason = normalized_domain
            if domain_reason and changed:
                normalized_domain = _idna_domain(original_domain)
                reverted, changed = True, False
        normalized = f"{local.lower()}@{normalized_domain}"
        cleaned, reason = sanitize_email(normalized)
        if not reason and changed:
            reason = "confusables-normalized"
        candidate = cleaned or normalized
        previous = src[max(0, match.start()-1):match.start()]
        suspicious = bool(
            prefix or (previous and (previous.isalpha() or previous == "."))
            or re.search(r"[A-Za-z]{12,}", local)
            or re.search(r"\d{7,}|\d{4}-\d{4}-", local)
            or re.search(r"[^\x00-\x7f]", domain.rsplit(".", 1)[-1])
            or re.search(r"(?:ru|com)[A-ZА-Я]", original_domain)
            or re.search(r"[^\x00-\x7f]", original_domain)
        )
        if suspicious and candidate not in suspects:
            suspects.append(candidate)
        items.append({"raw": raw, "normalized": normalized, "sanitized": cleaned,
                      "span": match.span(), "reason": reason, "reverted": reverted})
        if cleaned and cleaned not in found:
            found.append(cleaned)
    # Require the clean counterpart as evidence before discarding numeric glue.
    for address in list(found):
        shorter = re.sub(r"^\d+(?=[A-Za-z])", "", address)
        if shorter != address and shorter in found:
            found.remove(address)
            for item in items:
                if item["sanitized"] == address:
                    item["sanitized"], item["reason"] = "", "footnote-duplicate"
    meta = {"source": text, "normalized": src, "tokens": src.split(), "emails": found,
            "items": items, "suspects": suspects}
    return (found, meta) if return_meta else found


def extract_emails(text: str, return_meta: bool = False):
    """Compatibility entrypoint for the unified extractor."""
    return parse_emails_unified(text, return_meta=return_meta)


def canonicalize_email(addr: str) -> str:
    """Legacy canonical key, preserving the original provider domain alias."""
    local, domain = addr.strip().lower().split("@", 1)
    domain = _idna_domain(domain)
    return f"{_canonical_local(local, domain)}@{domain}"


def contains_url_but_not_email(text: str) -> bool:
    cleaned = preclean_for_email_extraction(text or "")
    if EMAIL_RE.search(cleaned):
        return False
    return bool(SAFE_URL_RE.search(cleaned))


# ---------------------------------------------------------------------------
#  Провайдер-aware каноникализация и дедупликация адресов
# ---------------------------------------------------------------------------

_PLUS_TAG_PROVIDERS = {
    "mail.ru",
    "gmail.com",
    "googlemail.com",
    "yandex.ru",
    "yandex.com",
    "yandex.ua",
    "yandex.by",
    "yandex.kz",
    "yandex.com.tr",
    "ya.ru",
    "outlook.com",
    "hotmail.com",
    "live.com",
}

_IGNORE_DOTS_PROVIDERS = {
    # Gmail/Googlemail игнорируют точки в local-part
    "gmail.com",
    "googlemail.com",
    # Яндекс/ya.ru тоже игнорирует точки
    "yandex.ru",
    "yandex.com",
    "yandex.ua",
    "yandex.by",
    "yandex.kz",
    "yandex.com.tr",
    "ya.ru",
}

_DOMAIN_ALIASES = {
    # Приводим к одному канону, чтобы не плодить варианты
    "googlemail.com": "gmail.com",
}


def _canonical_domain(dom: str) -> str:
    d = _idna_domain(dom.lower().strip())
    return _DOMAIN_ALIASES.get(d, d)


def _strip_plus_tag(local: str) -> str:
    # всё, что после первого '+', режем (стандартно для многих провайдеров)
    i = local.find("+")
    return local if i < 0 else local[:i]


def _canonical_local(local: str, domain: str) -> str:
    l = local.lower()
    d = domain.lower()
    if d in _PLUS_TAG_PROVIDERS:
        l = _strip_plus_tag(l)
    if d in _IGNORE_DOTS_PROVIDERS:
        l = l.replace(".", "")
    return l


def canonical_email(addr: str) -> str:
    """
    Каноникализация адреса: lowercase, IDNA для домена, провайдер-спец. правила.
    Предполагается, что вход уже синтаксически валиден.
    """

    local, dom = addr.split("@", 1)
    dom_c = _canonical_domain(dom)
    loc_c = _canonical_local(local, dom_c)
    return f"{loc_c}@{dom_c}"


def dedupe_with_variants(emails, return_map: bool = False):
    """
    Дедупликация с учётом провайдер-вариантов:
    - gmail/googlemail, yandex/ya: игнор точек в local, режем +tag
    - IDNA и lowercase для домена
    Возвращает:
      - список уникальных адресов с сохранением первого транспортного варианта;
      - при return_map=True кортеж (уникальные, mapping), где
        mapping[canonical] = {очищенные_варианты}.
    """

    uniques = []
    mapping: dict[str, set[str]] = {}
    for raw in emails or []:
        e, _ = sanitize_email(raw, _filter_policy=False)
        if not e:
            continue
        canon = canonical_email(e)
        if canon not in mapping:
            uniques.append(e)
        mapping.setdefault(canon, set()).add(e)
    if return_map:
        return uniques, mapping
    return uniques

# ---------------------------------------------------------------------------
#  Совместимость со старым кодом (legacy API)
# ---------------------------------------------------------------------------

def finalize_email(
    local_or_email: str,
    domain: str | None = None,
    *,
    raw_text: str | None = None,
    span: tuple[int, int] | None = None,
    sanitized: str | None = None,
    sanitize_reason: str | None = None,
) -> tuple[str, str | None, str]:
    """Normalize an address and return ``(email, reason, stage)``.

    The helper accepts either a full e-mail address as the first argument or a
    ``(local, domain)`` pair. A supplied sanitized address takes precedence;
    a previous rejection reason is preserved. Raw text and span are accepted
    for compatibility with pipeline callers.
    """

    if domain is None:
        addr = (local_or_email or "").strip()
    else:
        addr = f"{local_or_email}@{domain}"

    cleaned, reason = sanitize_email(sanitized or addr)
    if sanitize_reason and sanitize_reason != "confusables-normalized":
        return "", sanitize_reason, "sanitize"
    return cleaned, reason, "finalize"


def normalize_email(addr: str) -> str:
    """
    Ранее использовалась для «нормализации», теперь оборачивает canonical_email().
    Оставлена для совместимости со старыми импортами.
    """

    try:
        return canonical_email(addr)
    except Exception as e:
        logger.warning("normalize_email fallback for %r: %s", addr, e)
        return (addr or "").strip().lower()


def repair_email(addr: str) -> str:
    """
    Legacy: попытка «подлечить» адрес (обрезать пробелы, привести домен к IDNA).
    В новой логике — это просто canonical_email() c мягким фолбэком.
    """

    try:
        a = (addr or "").strip()
        # базовая подчистка типичных артефактов
        a = a.strip("()[]{}<>,;")
        return canonical_email(a)
    except Exception as e:
        logger.warning("repair_email fallback for %r: %s", addr, e)
        return (addr or "").strip().lower()


def sanitize_email(addr: str, strip_footnote: bool = True, *, _filter_policy: bool = True) -> tuple[str, str | None]:
    """
    Совместимый с легаси интерфейс:
      возвращает (cleaned, reason).
    - cleaned: очищенный транспортный адрес с сохранением точек и +tag,
               либо "" если адрес некорректный;
    - reason:  строковый код причины отказа (например, "invalid", "invalid-local",
               "invalid-domain"), либо None.
    Ключ для истории и дедупликации получается отдельно через canonical_email().
    """

    from utils.tld_utils import is_allowed_domain

    a = strip_invisibles(addr or "").strip().strip("()[]{}<>,;\"'`«»„“”‚‘’")
    if strip_footnote:
        a = re.sub(r"^[¹²³⁰⁴⁵⁶⁷⁸⁹①-⑳]+", "", a)
    a = re.sub(r"(?<=\w)-\r?\n(?=\w)", "", a)
    if a.count("@") != 1:
        return "", "invalid"
    local, domain = a.split("@", 1)
    domain = re.split(r"[?#]", domain, maxsplit=1)[0].rstrip(".")
    local = re.sub(r"\.{2,}", ".", local).strip("._-")
    if not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}", local):
        return "", "invalid-local"
    if re.match(r"(?:mail|yandex|gmail)\.(?:ru|com)[A-Za-z]", local, re.I):
        return "", "glued-domain-prefix"
    if _filter_policy and local.isdigit() and len(local) <= 3:
        return "", "looks-like-footnote"
    try:
        domain = idna.encode(domain.lower(), uts46=True).decode("ascii") if idna else domain.lower()
    except Exception:
        return "", "invalid-idna"
    if not re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9-]{2,63}", domain):
        return "", "invalid-domain"
    if not is_allowed_domain(domain) and os.getenv("REPAIR_TLD_TAIL", "1") == "1":
        base, tail = domain.rsplit(".", 1)
        # Retain the established conservative list of known PDF tail artefacts.
        repaired = re.fullmatch(r"(ru|com)(russia|com|net|sysoev|faculty|доцент|россия)", tail)
        if repaired:
            domain = f"{base}.{repaired.group(1)}"
    if _filter_policy and not is_allowed_domain(domain):
        return "", "tld-not-allowed"
    if _filter_policy and os.getenv("EMAIL_ROLE_PERSONAL_ONLY", "1") == "1" and (
        local.lower() in {"info", "support", "office", "contact", "admin", "noreply", "no-reply"}
        or local.lower().startswith("russia")
    ):
        return "", "role-like-prefix"
    return f"{local.lower()}@{domain}", None


def get_variants(addr: str):
    """
    Legacy: вернуть набор возможных вариантов адреса.
    Чтобы не раздувать список и не ломать старую логику,
    возвращаем минимально безопасный набор: только канонический и исходный.
    Если старый код ожидает множество/итерируемое — это совместимо.
    """

    try:
        canon = canonical_email(addr)
        base = (addr or "").strip().lower()
        s = {canon}
        if base and base != canon:
            s.add(base)
        return s
    except Exception:
        a = (addr or "").strip().lower()
        return {a} if a else set()


# Автоматическая проверка наличия ключевых экспортов


_FOOTNOTE_PREFIX_RE = re.compile(r"^(?:[\[(]?\d+[\])]?[\s_.:\-]*)+")


def _strip_leading_footnote(local: str) -> str:
    """Legacy helper: strip leading numeric footnote markers from ``local`` part."""

    if not local:
        return local
    return _FOOTNOTE_PREFIX_RE.sub("", local)


def _normalize_text(text: str) -> str:
    """Legacy wrapper delegating to :mod:`utils.text_normalize`."""

    from utils.text_normalize import normalize_text

    return normalize_text(text)
def _check_legacy_exports():
    required = {
        "dedupe_with_variants",
        "finalize_email",
        "normalize_email",
        "repair_email",
        "get_variants",
        "sanitize_email",
        "canonical_email",
        "drop_leading_char_twins",
        "drop_trailing_char_twins",
    }
    missing = [r for r in required if r not in globals()]
    if missing:
        logger.warning("email_clean: missing legacy exports: %s", missing)


_check_legacy_exports()

# ---------------------------------------------------------------------------
#  Экспорт через __all__ (на случай, если проект его использует)
# ---------------------------------------------------------------------------
try:
    __all__
except NameError:
    __all__ = []
if isinstance(__all__, (list, tuple, set)):
    if "dedupe_keep_original" not in __all__:
        try:
            __all__ = list(__all__) + ["dedupe_keep_original"]
        except Exception:
            pass

# Диагностика: при импорте выведем в лог факт наличия функции
try:
    logger.info("email_clean: dedupe_keep_original present: %s", "dedupe_keep_original" in globals())
except Exception:
    pass


# ---------------------------------------------------------------------------
# 🧩 Полный набор устаревших функций для совместимости со старым кодом
# ---------------------------------------------------------------------------

def is_valid_email(addr: str) -> bool:
    """Раньше проверяла валидность e-mail; теперь просто проверяем через EMAIL_RE."""
    if not addr:
        return False
    return bool(EMAIL_RE.fullmatch(addr.strip().lower()))


def strict_validate_domain(addr: str) -> bool:
    """Проверка домена по STRICT_DOMAIN_VALIDATE (из .env)."""
    try:
        if not addr or "@" not in addr:
            return False
        dom = addr.split("@", 1)[1]
        if os.getenv("STRICT_DOMAIN_VALIDATE", "1") == "1":
            return bool(re.fullmatch(r"[a-z0-9\-]+(\.[a-z0-9\-]+)+", dom.lower()))
        return True
    except Exception:
        return False


def looks_like_email(text: str) -> bool:
    """Простая проверка, похоже ли на e-mail (раньше использовалась в пайплайне)."""
    return bool(EMAIL_RE.search(text or ""))


def safe_parse_email(text: str):
    """Раньше возвращала нормализованный адрес или None при ошибке."""
    try:
        emails = parse_emails_unified(text)
        return emails[0] if emails else None
    except Exception:
        return None


def split_email(text: str):
    """Возвращает local и domain (старый интерфейс)."""
    try:
        local, dom = (text or "").split("@", 1)
        return local.strip(), dom.strip()
    except Exception:
        return "", ""


def strip_bad_chars(text: str) -> str:
    """Удаляет кавычки, пробелы, скобки вокруг e-mail."""
    return (text or "").strip("()[]{}<>,;\"'`«»„“”‚‘’ ")


def normalize_domain(dom: str) -> str:
    """Привести домен к IDNA / lowercase."""
    try:
        return _idna_domain(dom)
    except Exception:
        return (dom or "").lower()


def extract_possible_emails(text: str):
    """Раньше возвращала список всех найденных адресов (без нормализации)."""
    try:
        return EMAIL_RE.findall(preclean_for_email_extraction(text))
    except Exception:
        return []


def remove_bad_glyphs(text: str) -> str:
    """Удалить невидимые символы, zero-width, soft hyphens."""
    return strip_invisibles(text)


def normalize_confusables(text: str, domain: str | None = None):
    """Псевдоним для _normalize_confusables()."""
    if domain is None:
        return _normalize_confusables(text)
    local = _normalize_confusables(text)
    labels = domain.split(".")
    normalized_domain = ".".join(
        _normalize_confusables(label) if re.search(r"[A-Za-z]", label) and not label.startswith("xn--") else label
        for label in labels
    )
    return local, normalized_domain, (local, normalized_domain) != (text, domain)


def fix_confusables(text: str) -> str:
    """Ещё один синоним старой функции."""
    return _normalize_confusables(text)


def email_variants(addr: str):
    """Alias для get_variants()."""
    return get_variants(addr)


def clean_local_part(addr: str) -> str:
    """Вернуть только локальную часть (до @)."""
    return (addr or "").split("@", 1)[0].strip()


def safe_split_email(addr: str):
    """Alias split_email() для старых модулей."""
    return split_email(addr)

