import os

SEND_COOLDOWN_DAYS = int(os.getenv("SEND_COOLDOWN_DAYS", "180"))
SEND_STATS_PATH = os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")
DOMAIN_RATE_LIMIT_SEC = float(os.getenv("DOMAIN_RATE_LIMIT_SEC", "1.0"))
APPEND_TO_SENT = int(os.getenv("APPEND_TO_SENT", "1")) == 1
