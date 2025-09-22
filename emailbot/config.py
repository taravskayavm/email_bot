import os

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

# UX: разрешать редактирование сразу после предпросмотра?
ALLOW_EDIT_AT_PREVIEW = os.getenv("ALLOW_EDIT_AT_PREVIEW", "0") == "1"
