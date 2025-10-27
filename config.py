from pathlib import Path
import os

from emailbot.suppress_list import blocklist_path

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
    "LOCAL_TLDS",
    "LOCAL_DOMAINS_EXTRA",
    "ALLOW_FOREIGN_DEFAULT",
    "UI_STATE_PATH",
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
BLOCKED_EMAILS_PATH = blocklist_path()

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

# Локальные TLD (через запятую). Всё остальное считаем «иностранным», если не в allow-list доменов.
LOCAL_TLDS = [
    s.strip().lower() for s in os.getenv("LOCAL_TLDS", ".ru,.рф,.su").split(",") if s.strip()
]

# Явный allow-list доменов, считающихся «локальными» даже с не-локальным TLD.
# По умолчанию — gmail.com.
LOCAL_DOMAINS_EXTRA = {
    s.strip().lower()
    for s in os.getenv("LOCAL_DOMAINS_EXTRA", "gmail.com").split(",")
    if s.strip()
}

# Включать ли иностранные домены по умолчанию в «К отправке»
ALLOW_FOREIGN_DEFAULT = os.getenv("ALLOW_FOREIGN_DEFAULT", "0") == "1"

# Файл для хранения пользовательских UI-переключателей (без БД/Redis)
UI_STATE_PATH = Path(os.getenv("UI_STATE_PATH", "var/ui_state.json"))
UI_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
if not UI_STATE_PATH.exists():
    UI_STATE_PATH.write_text("{}", encoding="utf-8")
