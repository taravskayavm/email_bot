from pathlib import Path  # импорт Path для работы с путями файловой системы
import os  # импорт os для чтения переменных окружения
from typing import Set  # импорт Set для объявления множеств типов строк

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
    "EMAILBOT_SEND_DELAY_SEC",
    "LOCAL_DOMAINS_EXTRA_SET",
    "FORCE_FOREIGN_DOMAINS_SET",
    "EMAILBOT_TEST_MODE",
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
LOCAL_TLDS = [  # формируем список локальных TLD
    s.strip().lower()  # приводим запись TLD к нижнему регистру
    for s in os.getenv("LOCAL_TLDS", ".ru,.рф,.su,.com").split(",")  # читаем список локальных зон, добавлена .com
    if s.strip()  # исключаем пустые элементы
]

# Явный allow-list доменов, считающихся «локальными» даже с не-локальным TLD.
# По умолчанию — gmail.com.
LOCAL_DOMAINS_EXTRA: Set[str] = {  # набор доменов, которые считаются локальными независимо от TLD
    s.strip().lower()  # приводим домен к нормализованному виду
    for s in os.getenv("LOCAL_DOMAINS_EXTRA", "gmail.com").split(",")  # читаем CSV со списком доменов
    if s.strip()  # исключаем пустые значения
}  # завершили формирование набора дополнительных локальных доменов

# Включать ли иностранные домены по умолчанию в «К отправке»
ALLOW_FOREIGN_DEFAULT = os.getenv("ALLOW_FOREIGN_DEFAULT", "0") == "1"

# Файл для хранения пользовательских UI-переключателей (без БД/Redis)
UI_STATE_PATH = Path(os.getenv("UI_STATE_PATH", "var/ui_state.json"))
UI_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
if not UI_STATE_PATH.exists():
    UI_STATE_PATH.write_text("{}", encoding="utf-8")


def _parse_float_env(name: str, default: float) -> float:  # преобразуем переменную окружения в число с плавающей точкой
    """Возвращает значение переменной окружения как float, либо значение по умолчанию."""
    raw_value = os.environ.get(name, None)  # читаем строку из окружения или None
    if raw_value is None:  # проверяем отсутствие значения
        return default  # возвращаем значение по умолчанию, если переменной нет
    try:
        return float(str(raw_value).strip())  # приводим строку к float после обрезки пробелов
    except (TypeError, ValueError):
        return default  # при ошибке преобразования возвращаем значение по умолчанию


EMAILBOT_SEND_DELAY_SEC = _parse_float_env(  # определяем глобальную задержку отправки писем
    "EMAILBOT_SEND_DELAY_SEC",  # имя переменной окружения, задающей интервал
    6.0,  # значение по умолчанию в секундах
)

LOCAL_DOMAINS_EXTRA_SET: Set[str] = LOCAL_DOMAINS_EXTRA  # совместимое имя для набора локальных доменов

FORCE_FOREIGN_DOMAINS_SET: Set[str] = {  # набор доменов, которые всегда считаются иностранными
    s.strip().lower()  # домен приводим к нижнему регистру
    for s in os.environ.get("FORCE_FOREIGN_DOMAINS", "ion.ru").split(",")  # читаем обязательные foreign-домены
    if s.strip()  # фильтруем пустые строки
}  # завершаем формирование набора принудительно иностранных доменов

EMAILBOT_TEST_MODE = os.environ.get("EMAILBOT_TEST_MODE", "0") == "1"  # флаг тестового режима

# Совместимость: прежнее имя списка доменов с принудительной маркировкой как иностранные
FORCE_FOREIGN_DOMAINS = FORCE_FOREIGN_DOMAINS_SET  # возвращаем старое имя для кода, использующего прежнее обозначение
