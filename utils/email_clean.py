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
    "с": "s",
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
EMAIL_STRICT_VALIDATE_RE = re.compile(
    r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-zА-Яа-яЁё]{2,}$"
)

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
    return _INVISIBLES_RE.sub("", text or "")


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
    t = _normalize_confusables(t)
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
    return "".join(_deobfuscate_chunks([cleaned]))


def parse_emails_unified(text: str, return_meta: bool = False):
    """
    Нормализация (гомоглифы/невидимые) → деобфускация → извлечение e-mail → IDNA для домена.
    """

    src = preclean_for_email_extraction(text)
    raw_chunks = re.split(r"[,\s;\n]+", src)
    chunks = list(_deobfuscate_chunks(raw_chunks))

    found: Set[str] = set()
    items_map: dict[str, dict] = {}

    def _register(email: str, raw_value: str | None = None) -> None:
        if email in items_map:
            return
        item = {
            "raw": raw_value or email,
            "normalized": email,
            "sanitized": email,
            "span": None,
            "reason": None,
        }
        items_map[email] = item

    def _add_match(candidate: str) -> None:
        if not candidate:
            return
        for m in EMAIL_RE.finditer(candidate):
            raw_email = m.group(0)
            local, dom = raw_email.split("@", 1)
            normalized = f"{local.lower()}@{_idna_domain(dom)}"
            found.add(normalized)
            _register(normalized, raw_email)

    for tok in chunks:
        if not tok:
            continue
        variants = {tok}
        collapsed = _collapse_spaced_tokens(tok)
        if collapsed:
            variants.add(collapsed)
        trimmed = drop_leading_char_twins(tok)
        trimmed = drop_trailing_char_twins(trimmed)
        if trimmed:
            variants.add(trimmed)
            collapsed_trimmed = _collapse_spaced_tokens(trimmed)
            if collapsed_trimmed:
                variants.add(collapsed_trimmed)
        for candidate in variants:
            _add_match(candidate)

    for m in OBFUSCATED_EMAIL_RE.finditer(src):
        local_raw = m.group(1) or ""
        sep = m.group(2) or ""
        domain_raw = m.group(3) or ""
        cand = f"{local_raw}{'@' if '@' in sep else '@'}{domain_raw}"
        cand = _collapse_spaced_tokens(cand)
        if not cand or "@" not in cand:
            continue
        _add_match(cand)

    res = sorted(found)
    if return_meta:
        items = [items_map[email] for email in res]
        return (
            res,
            {"source": text, "normalized": src, "tokens": chunks, "emails": res, "items": items},
        )
    return res


def contains_url_but_not_email(text: str) -> bool:
    cleaned = preclean_for_email_extraction(text or "")
    if EMAIL_RE.search(cleaned):
        return False
    return bool(SAFE_URL_RE.search(cleaned))


# ---------------------------------------------------------------------------
#  Провайдер-aware каноникализация и дедупликация адресов
# ---------------------------------------------------------------------------

_PLUS_TAG_PROVIDERS = {
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
      - список уникальных адресов (в каноническом виде);
      - при return_map=True кортеж (уникальные, mapping), где
        mapping[canonical] = {варианты_как_вводились}.
    """

    if not emails:
        return ([], {}) if return_map else []
    mapping: dict[str, set[str]] = {}
    for raw in emails:
        e = (raw or "").strip().lower()
        if not e or "@" not in e:
            continue
        try:
            local, dom = e.split("@", 1)
        except ValueError:
            continue
        dom_c = _canonical_domain(dom)
        loc_c = _canonical_local(local, dom_c)
        canon = f"{loc_c}@{dom_c}"
        mapping.setdefault(canon, set()).add(e)
    uniques = sorted(mapping.keys())
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
    ``(local, domain)`` pair. Additional keyword arguments are accepted for
    backward compatibility with pipeline callers and are ignored here.
    """

    if domain is None:
        addr = (local_or_email or "").strip()
    else:
        addr = f"{local_or_email}@{domain}"

    try:
        cleaned = canonical_email(addr)
    except Exception as exc:
        logger.warning("finalize_email fallback for %r: %s", addr, exc)
        cleaned = (addr or "").strip().lower()

    if not cleaned or "@" not in cleaned:
        return "", "invalid", "finalize"
    return cleaned, None, "finalize"


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


def sanitize_email(addr: str, strip_footnote: bool = True) -> tuple[str, str | None]:
    """
    Совместимый с легаси интерфейс:
      возвращает (cleaned, reason).
    - cleaned: нормализованный e-mail в канонической форме (ключ для кулдауна/дедупа),
               либо "" если адрес некорректный;
    - reason:  строковый код причины отказа (например, "invalid", "invalid-local",
               "invalid-domain"), либо None.
    ВАЖНО: ЭТО НЕ «транспортный» адрес для SMTP. Для фактической отправки используйте
    адрес пользователя без удаления точек/плюсов (см. emailbot.messaging).
    """

    try:
        a = (addr or "").strip()
        a = a.strip("()[]{}<>,;\"'`«»„“”‚‘’")
        if not a or "@" not in a:
            return "", "invalid"
        local, sep, domain = a.partition("@")
        if not sep:
            return "", "invalid"
        if not local:
            return "", "invalid-local"
        if not domain:
            return "", "invalid-domain"
        try:
            cleaned = canonical_email(a)
        except Exception:
            cleaned = a.strip().lower()
        if not cleaned or "@" not in cleaned:
            return "", "invalid"
        return cleaned, None
    except Exception as e:
        logger.warning("sanitize_email fallback for %r: %s", addr, e)
        return "", "invalid"


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
    """Return ``True`` if ``addr`` looks like a syntactically valid e-mail."""
    if not addr or "@" not in addr:
        return False
    candidate = addr.strip()
    if not candidate or "@" not in candidate:
        return False
    local, _, domain = candidate.rpartition("@")
    if not local or not domain or "." not in domain:
        return False
    return bool(EMAIL_STRICT_VALIDATE_RE.match(candidate))


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


def normalize_confusables(text: str) -> str:
    """Псевдоним для _normalize_confusables()."""
    return _normalize_confusables(text)


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

