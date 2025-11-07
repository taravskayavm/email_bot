import os
from pathlib import Path

from emailbot.runtime_config import get as rc_get


def _int(name: str, default: int) -> int:
    """Read integer environment variables with graceful fallback."""

    try:
        raw = os.getenv(name, "")
        return int(raw.strip() or default)
    except Exception:
        return default


def _float(name: str, default: float) -> float:
    """Read float environment variables with graceful fallback."""

    try:
        raw = os.getenv(name, "")
        return float(raw.strip() or default)
    except Exception:
        return default


def _str(name: str, default: str) -> str:
    """Read string environment variables with stripping."""

    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    return raw if raw else default


SEND_COOLDOWN_DAYS = int(os.getenv("SEND_COOLDOWN_DAYS", "180"))
SEND_STATS_PATH = os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
DOMAIN_RATE_LIMIT_SEC = float(os.getenv("DOMAIN_RATE_LIMIT_SEC", "1.0"))
APPEND_TO_SENT = int(os.getenv("APPEND_TO_SENT", "1")) == 1
CRAWL_MAX_PAGES = int(os.getenv("CRAWL_MAX_PAGES", "120"))
CRAWL_MAX_DEPTH = int(os.getenv("CRAWL_MAX_DEPTH", "3"))
CRAWL_SAME_DOMAIN = os.getenv("CRAWL_SAME_DOMAIN", "1") == "1"
CRAWL_DELAY_SEC = float(os.getenv("CRAWL_DELAY_SEC", "0.5"))
CRAWL_USER_AGENT = os.getenv(
    "CRAWL_USER_AGENT", "EmailBotCrawler/1.0 (+contact@example.com)"
)
CRAWL_HTTP2 = os.getenv("CRAWL_HTTP2", "1") == "1"
CRAWL_MAX_PAGES_PER_DOMAIN = int(os.getenv("CRAWL_MAX_PAGES_PER_DOMAIN", "50"))
CRAWL_TIME_BUDGET_SECONDS = int(os.getenv("CRAWL_TIME_BUDGET_SECONDS", "120"))
ROBOTS_CACHE_PATH = os.getenv("ROBOTS_CACHE_PATH", "var/robots_cache.json")
ROBOTS_CACHE_TTL_SECONDS = int(os.getenv("ROBOTS_CACHE_TTL_SECONDS", "86400"))

# UX: разрешать редактирование сразу после предпросмотра?
ALLOW_EDIT_AT_PREVIEW = os.getenv("ALLOW_EDIT_AT_PREVIEW", "0") == "1"

# Отключение встроенного (инлайн) редактора e-mail в боте
ENABLE_INLINE_EMAIL_EDITOR = os.getenv("ENABLE_INLINE_EMAIL_EDITOR", "0") == "1"

# PDF extraction tuning
PDF_ENGINE = _str("EMAILBOT_PDF_ENGINE", "fitz")
PDF_MAX_PAGES = rc_get("PDF_MAX_PAGES", _int("PDF_MAX_PAGES", 40))
# Фиксированный таймаут остаётся как резервный (если адаптивный выключен)
PDF_EXTRACT_TIMEOUT = rc_get(
    "PDF_EXTRACT_TIMEOUT", _int("PDF_EXTRACT_TIMEOUT", 25)
)  # seconds
EMAILBOT_ENABLE_OCR = rc_get(
    "EMAILBOT_ENABLE_OCR", _int("EMAILBOT_ENABLE_OCR", 0) == 1
)
# -------- PDF / OCR Auto Mode --------
PDF_OCR_AUTO = rc_get("PDF_OCR_AUTO", _int("PDF_OCR_AUTO", 1))
PDF_OCR_PROBE_PAGES = rc_get("PDF_OCR_PROBE_PAGES", _int("PDF_OCR_PROBE_PAGES", 5))
PDF_OCR_MAX_PAGES = rc_get("PDF_OCR_MAX_PAGES", _int("PDF_OCR_MAX_PAGES", 30))
PDF_OCR_MIN_TEXT_RATIO = rc_get(
    "PDF_OCR_MIN_TEXT_RATIO", _float("PDF_OCR_MIN_TEXT_RATIO", 0.05)
)
PDF_OCR_MIN_CHARS = rc_get("PDF_OCR_MIN_CHARS", _int("PDF_OCR_MIN_CHARS", 150))
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "").strip()

# -------- Ранний прогресс / тёплый старт --------
PDF_WARMUP_PAGES = rc_get("PDF_WARMUP_PAGES", _int("PDF_WARMUP_PAGES", 3))
PDF_EARLY_HEARTBEAT_SEC = rc_get(
    "PDF_EARLY_HEARTBEAT_SEC", _float("PDF_EARLY_HEARTBEAT_SEC", 3.0)
)

# -------- OCR / PDF unified knobs --------
PDF_BACKEND = rc_get(
    "PDF_BACKEND",
    (_str("PDF_BACKEND", PDF_ENGINE) or PDF_ENGINE).strip().lower(),
)
if PDF_BACKEND not in {"fitz", "pdfminer", "auto"}:
    PDF_BACKEND = "fitz"

PDF_LEGACY_MODE = rc_get("PDF_LEGACY_MODE", _int("LEGACY_MODE", 0) == 1)
PDF_FAST_MIN_HITS = rc_get("PDF_FAST_MIN_HITS", _int("PDF_FAST_MIN_HITS", 8))
PDF_FAST_TIMEOUT_MS = rc_get("PDF_FAST_TIMEOUT_MS", _int("PDF_FAST_TIMEOUT_MS", 60))
PDF_TEXT_TRUNCATE_LIMIT = rc_get(
    "PDF_TEXT_TRUNCATE_LIMIT", _int("PDF_TEXT_TRUNCATE_LIMIT", 2_000_000)
)

PDF_OCR_ENGINE = rc_get(
    "PDF_OCR_ENGINE",
    _str("PDF_OCR_ENGINE", _str("OCR_ENGINE", "pytesseract")),
)
PDF_OCR_LANG = rc_get(
    "PDF_OCR_LANG",
    _str("PDF_OCR_LANG", _str("OCR_LANG", "eng+rus")),
)
PDF_OCR_PAGE_LIMIT = rc_get(
    "PDF_OCR_PAGE_LIMIT",
    _int("PDF_OCR_PAGE_LIMIT", PDF_OCR_MAX_PAGES if PDF_OCR_MAX_PAGES > 0 else 10),
)
PDF_OCR_TIME_LIMIT = rc_get("PDF_OCR_TIME_LIMIT", _int("PDF_OCR_TIME_LIMIT", 30))
PDF_OCR_TIMEOUT_PER_PAGE = rc_get(
    "PDF_OCR_TIMEOUT_PER_PAGE", _int("PDF_OCR_TIMEOUT_PER_PAGE", 12)
)
PDF_OCR_DPI = rc_get("PDF_OCR_DPI", _int("PDF_OCR_DPI", 300))
PDF_OCR_CACHE_DIR = rc_get(
    "PDF_OCR_CACHE_DIR",
    str(Path(_str("PDF_OCR_CACHE_DIR", _str("OCR_CACHE_DIR", "var/ocr_cache")))).strip(),
)
PDF_OCR_ALLOW_BEST_EFFORT = rc_get(
    "PDF_OCR_ALLOW_BEST_EFFORT",
    _int("PDF_OCR_ALLOW_BEST_EFFORT", 1) == 1,
)
PDF_FORCE_OCR_IF_FOUND_LT = rc_get(
    "PDF_FORCE_OCR_IF_FOUND_LT", _int("PDF_FORCE_OCR_IF_FOUND_LT", 25)
)


# -------- PDF Open Guard / Fallback --------
PDF_OPEN_TIMEOUT_SEC = rc_get(
    "PDF_OPEN_TIMEOUT_SEC",
    _int("PDF_OPEN_TIMEOUT_SEC", 10),
)
PDF_FALLBACK_BACKEND = rc_get(
    "PDF_FALLBACK_BACKEND",
    os.getenv("PDF_FALLBACK_BACKEND", "pdfminer"),
)

# 📈 Адаптивный таймаут (включён по умолчанию)
PDF_ADAPTIVE_TIMEOUT = rc_get("PDF_ADAPTIVE_TIMEOUT", os.getenv("PDF_ADAPTIVE_TIMEOUT", "1") == "1")
# базовая часть таймаута, сек
PDF_TIMEOUT_BASE = rc_get("PDF_TIMEOUT_BASE", _int("PDF_TIMEOUT_BASE", 15))
# добавка за каждый мегабайт, сек/МБ
PDF_TIMEOUT_PER_MB = rc_get("PDF_TIMEOUT_PER_MB", _float("PDF_TIMEOUT_PER_MB", 0.6))
# минимальный и максимальный пределы, сек
PDF_TIMEOUT_MIN = rc_get("PDF_TIMEOUT_MIN", _int("PDF_TIMEOUT_MIN", 15))
PDF_TIMEOUT_MAX = rc_get("PDF_TIMEOUT_MAX", _int("PDF_TIMEOUT_MAX", 90))

# -------- Поведение парсинга --------
# Если 1 — парсим все страницы PDF (без раннего выхода по "достаточно адресов")
PARSE_COLLECT_ALL = rc_get("PARSE_COLLECT_ALL", _int("PARSE_COLLECT_ALL", 1))

# Частота обновления прогресса в Telegram
PROGRESS_UPDATE_EVERY_PAGES = rc_get(
    "PROGRESS_UPDATE_EVERY_PAGES", _int("PROGRESS_UPDATE_EVERY_PAGES", 10)
)
PROGRESS_UPDATE_MIN_SEC = rc_get(
    "PROGRESS_UPDATE_MIN_SEC", _float("PROGRESS_UPDATE_MIN_SEC", 2.0)
)
