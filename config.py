from pathlib import Path
import os

__all__ = [
    "CONFUSABLES_NORMALIZE",
    "OBFUSCATION_ENABLE",
    "get_bool",
    "BLOCKED_EMAILS_PATH",
    "EMAIL_ADDRESS",
    "EMAIL_PASSWORD",
    "IMAP_HOST",
    "IMAP_PORT",
    "INBOX_MAILBOX",
    "BOUNCE_SINCE_DAYS",
]


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


BASE_DIR = Path(__file__).resolve().parent

CONFUSABLES_NORMALIZE = get_bool("CONFUSABLES_NORMALIZE", False)
OBFUSCATION_ENABLE = get_bool("OBFUSCATION_ENABLE", False)

# Путь к файлу заблокированных адресов (var/blocked_emails.txt по умолчанию)
BLOCKED_EMAILS_PATH = Path(
    os.getenv("BLOCKED_EMAILS_PATH", str(BASE_DIR / "var" / "blocked_emails.txt"))
)

# Гарантируем, что каталог и файл существуют
BLOCKED_EMAILS_PATH.parent.mkdir(parents=True, exist_ok=True)
if not BLOCKED_EMAILS_PATH.exists():
    BLOCKED_EMAILS_PATH.touch()

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
IMAP_HOST = os.getenv("IMAP_HOST", "imap.mail.ru")
IMAP_PORT = _get_int("IMAP_PORT", 993)
INBOX_MAILBOX = os.getenv("INBOX_MAILBOX", "INBOX")
BOUNCE_SINCE_DAYS = _get_int("BOUNCE_SINCE_DAYS", 7)
