from __future__ import annotations

import logging
import os
import re
import unicodedata
from typing import Iterable

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

EMAIL_RE = re.compile(r"(?ix)\b" r"[a-z0-9._%+\-]+@(?:[a-z0-9\-]+\.)+[a-z0-9\-]{2,}" r"\b")

# ВАЖНО: (?<!@) — чтобы доменная часть e-mail не считалась ссылкой
SAFE_URL_RE = re.compile(
    r"(?ix)(?<!@)\b((?:https?://)?(?:www\.)?[^\s<>()]+?\.[^\s<>()]{2,}[^\s<>()]*)(?=$|[\s,;:!?)}\]])"
)

# «Мусор» на краях токена: пробелы, NBSP/soft hyphen, пунктуация, кавычки, тире, маркеры списков
_LEADING_JUNK_RE = re.compile(
    r'^[\s\u00A0\u00AD\.\-–—·•_*~=:;|/\\<>\(\)\[\]\{\}"\'`«»„“”‚‘’]+'
)
_TRAILING_JUNK_RE = re.compile(
    r'[\s\u00A0\u00AD\.\-–—·•_*~=:;|/\\<>\(\)\[\]\{\}"\'`«»„“”‚‘’]+$'
)
def _strip_leading_token_junk(token: str) -> str:
    if not token:
        return token
    return _LEADING_JUNK_RE.sub("", token)


def _strip_trailing_token_junk(token: str) -> str:
    if not token:
        return token
    return _TRAILING_JUNK_RE.sub("", token)


def drop_leading_char_twins(emails: Iterable[str] | str) -> list[str] | str:
    """Legacy helper working on lists of e-mails.

    Historically this helper accepted ``list[str]`` and removed "twin" entries where a
    domain shared two local-parts differing only by the very first character. Some
    pipelines still import it for that deduplication step. Recent refactors also used
    the name for per-token trimming, so we now support both call styles: passing a
    single ``str`` keeps the lightweight punctuation stripping behaviour, while any
    iterable retains the original list-processing semantics.
    """

    if isinstance(emails, str):
        return _strip_leading_token_junk(emails)

    email_list = list(emails)

    # Historically this helper was effectively a no-op on iterables: callers expected
    # to receive the original list (possibly copied) without any deduplication. The
    # newer "twin" removal logic turned out to be overly aggressive and pruned
    # legitimate addresses that merely shared a suffix in their local-part. To maintain
    # backwards compatibility we simply return a shallow copy of the input sequence.
    return email_list


def drop_trailing_char_twins(s: str) -> str:
    """Legacy helper retained for backwards compatibility with older imports."""

    return _strip_trailing_token_junk(s)


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
    t = re.sub(r"[ \t]+", " ", t)
    return t.strip()


def _deobfuscate_chunks(chunks: Iterable[str]) -> Iterable[str]:
    if not OBFUSCATION_ENABLE:
        yield from chunks
        return
    subs = [
        (re.compile(r"(?i)\s*(?:\[at\]|\(at\)|\{at\}| at | собака )\s*"), "@"),
        (re.compile(r"(?i)\s*(?:\[dot\]|\(dot\)|\{dot\}| dot | точка )\s*"), "."),
    ]
    for s in chunks:
        t = s
        for rx, rep in subs:
            t = rx.sub(rep, t)
        yield t


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

    found = set()
    for tok in chunks:
        if not tok:
            continue
        m = EMAIL_RE.search(tok)
        if m:
            local, dom = m.group(0).split("@", 1)
            found.add(f"{local.lower()}@{_idna_domain(dom)}")
            continue
        # попытка вытащить из токена без лишних обрамлений
        # мягкая подчистка краёв (совместимо со старым пайплайном)
        core = _strip_trailing_token_junk(_strip_leading_token_junk(tok))
        for m in EMAIL_RE.finditer(core):
            local, dom = m.group(0).split("@", 1)
            found.add(f"{local.lower()}@{_idna_domain(dom)}")

    res = sorted(found)
    if return_meta:
        return res, {"source": text, "normalized": src, "tokens": chunks, "emails": res}
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

def finalize_email(addr: str) -> str:
    """
    Backward-compatible stub.
    Старые версии pipelines/extract_emails.py и messaging.py вызывали finalize_email
    для нормализации адресов. Теперь это делегируется canonical_email().
    """
    try:
        return canonical_email(addr)
    except Exception as e:
        logger.warning("finalize_email fallback for %r: %s", addr, e)
        return (addr or "").strip().lower()


# Автоматическая проверка наличия ключевых экспортов
def _check_legacy_exports():
    required = {
        "dedupe_with_variants",
        "finalize_email",
        "canonical_email",
        "drop_leading_char_twins",
        "drop_trailing_char_twins",
    }
    missing = [r for r in required if r not in globals()]
    if missing:
        logger.warning("email_clean: missing legacy exports: %s", missing)


_check_legacy_exports()

