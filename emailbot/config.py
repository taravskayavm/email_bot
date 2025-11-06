import os
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
PDF_ENGINE = os.getenv("EMAILBOT_PDF_ENGINE", "fitz")
PDF_MAX_PAGES = rc_get("PDF_MAX_PAGES", _int("PDF_MAX_PAGES", 40))
# Фиксированный таймаут остаётся как резервный (если адаптивный выключен)
PDF_EXTRACT_TIMEOUT = rc_get("PDF_EXTRACT_TIMEOUT", _int("PDF_EXTRACT_TIMEOUT", 25))  # seconds
EMAILBOT_ENABLE_OCR = rc_get(
    "EMAILBOT_ENABLE_OCR", os.getenv("EMAILBOT_ENABLE_OCR", "0") == "1"
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
