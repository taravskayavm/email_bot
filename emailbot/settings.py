"""Runtime configurable settings for the bot."""

from __future__ import annotations

from pathlib import Path
import json
import os
import logging

from . import settings_store as _store

logger = logging.getLogger(__name__)

# [EBOT-083] Загружаем .env как можно раньше, чтобы переменные окружения были доступны
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

# [EBOT-078] Унификация числа воркеров и обратная совместимость
def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# Новый «канонический» параметр для рассылки
SEND_MAX_WORKERS = _int_env("SEND_MAX_WORKERS", _int_env("MAX_WORKERS", 4))
if SEND_MAX_WORKERS < 1:
    logger.warning("settings: SEND_MAX_WORKERS=%r looks invalid; forcing to 1", SEND_MAX_WORKERS)
    SEND_MAX_WORKERS = 1

# Deprecated, но оставляем для обратной совместимости (см. EBOT-075)
try:
    _legacy_parse = _int_env("PARSE_MAX_WORKERS", SEND_MAX_WORKERS)
    if "PARSE_MAX_WORKERS" in os.environ:
        logger.warning("settings: PARSE_MAX_WORKERS is deprecated; use SEND_MAX_WORKERS instead")
except Exception:
    _legacy_parse = SEND_MAX_WORKERS

# Экспортируем для старых модулей (и нового кода тоже безопасно)
PARSE_MAX_WORKERS = _legacy_parse

# [EBOT-083] Канонический таймаут для файловых операций при отправке
SEND_FILE_TIMEOUT = _int_env("SEND_FILE_TIMEOUT", _int_env("FILE_TIMEOUT", 20))
# Backward-compat: некоторые модули могут импортировать PARSE_FILE_TIMEOUT
PARSE_FILE_TIMEOUT = _int_env("PARSE_FILE_TIMEOUT", SEND_FILE_TIMEOUT)

# Информативный лог итоговых значений (1 раз на импорт)
try:
    logger.info(
        "settings: SEND_MAX_WORKERS=%s; PARSE_MAX_WORKERS=%s; SEND_FILE_TIMEOUT=%s; PARSE_FILE_TIMEOUT=%s",
        SEND_MAX_WORKERS,
        PARSE_MAX_WORKERS,
        SEND_FILE_TIMEOUT,
        PARSE_FILE_TIMEOUT,
    )
except Exception:
    pass


# Default values
STRICT_OBFUSCATION: bool = True
FOOTNOTE_RADIUS_PAGES: int = 1
PDF_LAYOUT_AWARE: bool = False
ENABLE_OCR: bool = False
ENABLE_PROVIDER_CANON: bool = os.getenv("ENABLE_PROVIDER_CANON", "1") == "1"
CANON_GMAIL_DOTS: bool = os.getenv("CANON_GMAIL_DOTS", "1") == "1"
CANON_GMAIL_PLUS: bool = os.getenv("CANON_GMAIL_PLUS", "1") == "1"
CANON_OTHER_PLUS: bool = os.getenv("CANON_OTHER_PLUS", "0") == "1"
MAX_ASSETS: int = 8
MAX_SITEMAP_URLS: int = 200
MAX_DOCS: int = 30
PER_REQUEST_TIMEOUT: int = 15
DAILY_SEND_LIMIT: int = 300
EXTERNAL_SOURCES: dict[str, dict[str, dict[str, str]]] = {}
# UI helpers
SKIPPED_PREVIEW_LIMIT: int = int(os.getenv("SKIPPED_PREVIEW_LIMIT", "10"))
LAST_SUMMARY_DIR: str = os.getenv("LAST_SUMMARY_DIR", "var/last_summaries")
# Отчётная временная зона (используется в логах/отчётах)
REPORT_TZ: str = (os.getenv("REPORT_TZ") or "Europe/Moscow").strip() or "Europe/Moscow"


# ---- Reconcile (IMAP vs CSV) ----
RECONCILE_SINCE_DAYS: int = int(os.getenv("RECONCILE_SINCE_DAYS", "7"))

# Краулер: бюджеты и кэш
CRAWL_MAX_PAGES_PER_DOMAIN = int(os.getenv("CRAWL_MAX_PAGES_PER_DOMAIN", "50"))
CRAWL_TIME_BUDGET_SECONDS = int(os.getenv("CRAWL_TIME_BUDGET_SECONDS", "120"))
ROBOTS_CACHE_PATH = os.getenv("ROBOTS_CACHE_PATH", "var/robots_cache.json")
ROBOTS_CACHE_TTL_SECONDS = int(os.getenv("ROBOTS_CACHE_TTL_SECONDS", "86400"))

TPL_DIR = Path("templates")
LABELS_FILE = TPL_DIR / "_labels.json"


def load() -> None:
    """Load configuration from the persistent store."""

    global STRICT_OBFUSCATION, FOOTNOTE_RADIUS_PAGES, PDF_LAYOUT_AWARE, ENABLE_OCR
    global ENABLE_PROVIDER_CANON, CANON_GMAIL_DOTS, CANON_GMAIL_PLUS, CANON_OTHER_PLUS
    global MAX_ASSETS, MAX_SITEMAP_URLS, MAX_DOCS, PER_REQUEST_TIMEOUT
    global EXTERNAL_SOURCES, DAILY_SEND_LIMIT, SKIPPED_PREVIEW_LIMIT, LAST_SUMMARY_DIR
    STRICT_OBFUSCATION = bool(_store.get("STRICT_OBFUSCATION", STRICT_OBFUSCATION))
    FOOTNOTE_RADIUS_PAGES = int(_store.get("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES))
    PDF_LAYOUT_AWARE = bool(_store.get("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE))
    ENABLE_OCR = bool(_store.get("ENABLE_OCR", ENABLE_OCR))
    ENABLE_PROVIDER_CANON = bool(
        _store.get("ENABLE_PROVIDER_CANON", ENABLE_PROVIDER_CANON)
    )
    CANON_GMAIL_DOTS = bool(_store.get("CANON_GMAIL_DOTS", CANON_GMAIL_DOTS))
    CANON_GMAIL_PLUS = bool(_store.get("CANON_GMAIL_PLUS", CANON_GMAIL_PLUS))
    CANON_OTHER_PLUS = bool(_store.get("CANON_OTHER_PLUS", CANON_OTHER_PLUS))
    MAX_ASSETS = int(_store.get("MAX_ASSETS", MAX_ASSETS))
    MAX_SITEMAP_URLS = int(_store.get("MAX_SITEMAP_URLS", MAX_SITEMAP_URLS))
    MAX_DOCS = int(_store.get("MAX_DOCS", MAX_DOCS))
    PER_REQUEST_TIMEOUT = int(_store.get("PER_REQUEST_TIMEOUT", PER_REQUEST_TIMEOUT))
    DAILY_SEND_LIMIT = int(_store.get("DAILY_SEND_LIMIT", DAILY_SEND_LIMIT))
    EXTERNAL_SOURCES = _store.get("EXTERNAL_SOURCES", EXTERNAL_SOURCES) or {}
    SKIPPED_PREVIEW_LIMIT = int(
        _store.get("SKIPPED_PREVIEW_LIMIT", SKIPPED_PREVIEW_LIMIT)
    )
    LAST_SUMMARY_DIR = str(_store.get("LAST_SUMMARY_DIR", LAST_SUMMARY_DIR))


def save() -> None:
    """Persist current configuration."""

    _store.set("STRICT_OBFUSCATION", STRICT_OBFUSCATION)
    _store.set("FOOTNOTE_RADIUS_PAGES", FOOTNOTE_RADIUS_PAGES)
    _store.set("PDF_LAYOUT_AWARE", PDF_LAYOUT_AWARE)
    _store.set("ENABLE_OCR", ENABLE_OCR)
    _store.set("ENABLE_PROVIDER_CANON", ENABLE_PROVIDER_CANON)
    _store.set("CANON_GMAIL_DOTS", CANON_GMAIL_DOTS)
    _store.set("CANON_GMAIL_PLUS", CANON_GMAIL_PLUS)
    _store.set("CANON_OTHER_PLUS", CANON_OTHER_PLUS)
    _store.set("MAX_ASSETS", MAX_ASSETS)
    _store.set("MAX_SITEMAP_URLS", MAX_SITEMAP_URLS)
    _store.set("MAX_DOCS", MAX_DOCS)
    _store.set("PER_REQUEST_TIMEOUT", PER_REQUEST_TIMEOUT)
    _store.set("DAILY_SEND_LIMIT", DAILY_SEND_LIMIT)
    _store.set("EXTERNAL_SOURCES", EXTERNAL_SOURCES)
    _store.set("SKIPPED_PREVIEW_LIMIT", SKIPPED_PREVIEW_LIMIT)
    _store.set("LAST_SUMMARY_DIR", LAST_SUMMARY_DIR)


def _load_labels() -> dict[str, dict[str, str]]:
    if not LABELS_FILE.exists():
        return {}
    try:
        data = json.loads(LABELS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def list_available_directions() -> list[str]:
    """Return direction labels from templates/_labels.json."""

    labels = _load_labels()
    return [str(meta.get("label") or slug) for slug, meta in labels.items()]


def resolve_label(label: str) -> str:
    """Resolve human-readable label back to slug."""

    query = label.strip()
    for slug, meta in _load_labels().items():
        stored_label = str(meta.get("label") or "").strip()
        if stored_label == query:
            return slug
    return query


# Load settings on module import.
load()


__all__ = [
    "STRICT_OBFUSCATION",
    "FOOTNOTE_RADIUS_PAGES",
    "PDF_LAYOUT_AWARE",
    "ENABLE_OCR",
    "ENABLE_PROVIDER_CANON",
    "CANON_GMAIL_DOTS",
    "CANON_GMAIL_PLUS",
    "CANON_OTHER_PLUS",
    "MAX_ASSETS",
    "MAX_SITEMAP_URLS",
    "MAX_DOCS",
    "PER_REQUEST_TIMEOUT",
    "DAILY_SEND_LIMIT",
    "EXTERNAL_SOURCES",
    "SKIPPED_PREVIEW_LIMIT",
    "LAST_SUMMARY_DIR",
    "REPORT_TZ",
    "RECONCILE_SINCE_DAYS",
    "CRAWL_MAX_PAGES_PER_DOMAIN",
    "CRAWL_TIME_BUDGET_SECONDS",
    "ROBOTS_CACHE_PATH",
    "ROBOTS_CACHE_TTL_SECONDS",
    "SEND_MAX_WORKERS",
    "PARSE_MAX_WORKERS",
    "SEND_FILE_TIMEOUT",
    "PARSE_FILE_TIMEOUT",
    "load",
    "save",
    "list_available_directions",
    "resolve_label",
]

