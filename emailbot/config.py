import os  # Читаем переменные окружения
from pathlib import Path  # Работаем с путями к файлам и каталогам
from zoneinfo import ZoneInfo  # Поддерживаем часовые пояса стандартными средствами

from emailbot.runtime_config import get as rc_get  # Загружаем динамические настройки


class _PytzCompat:  # Создаём локальную совместимость с минимальным API pytz
    """Provide the ``timezone`` method used by legacy settings."""

    @staticmethod  # Метод не зависит от состояния объекта совместимости
    def timezone(name: str) -> ZoneInfo:  # Возвращаем стандартную таймзону по имени
        """Return a ``ZoneInfo`` timezone for ``name``."""

        return ZoneInfo(name)  # Делегируем создание часового пояса стандартной библиотеке


pytz = _PytzCompat()  # Сохраняем прежнее имя для существующего кода ниже


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


def _env_bool(*keys: str, default: int = 0) -> int:  # Объявляем совместимый парсер булевых флагов
    """Return 1/0 for the first present boolean environment variable."""  # Описываем предназначение функции

    for key in keys:  # Перебираем возможные имена переменных окружения
        value = os.getenv(key)  # Считываем значение по текущему ключу
        if value is not None:  # Проверяем, найдено ли значение
            try:  # Пытаемся безопасно преобразовать его к булевому виду
                normalized = str(value).strip().lower()  # Нормализуем строку к нижнему регистру
                return 1 if normalized in {"1", "true", "on", "yes"} else 0  # Возвращаем 1 только для истинных значений
            except Exception:  # Отлавливаем любые ошибки преобразования
                continue  # Пропускаем некорректное значение и проверяем следующий ключ
    return default  # Возвращаем значение по умолчанию, если ничего не подошло


def _env_str(*keys: str, default: str = "") -> str:  # Создаём унифицированный парсер строковых значений
    """Return the first present string environment variable without stripping semantics."""  # Даём пояснение функции

    for key in keys:  # Итерируем по всем возможным именам
        value = os.getenv(key)  # Считываем значение по текущему ключу
        if value is not None:  # Проверяем, что переменная определена
            return str(value)  # Возвращаем строковое представление найденного значения
    return default  # Если ничего не найдено, возвращаем дефолт


def _env_int(*keys: str, default: int = 0) -> int:  # Вводим парсер целочисленных значений с совместимостью имён
    """Return the first present integer environment variable."""  # Описываем работу функции

    for key in keys:  # Перебираем допустимые ключи
        value = os.getenv(key)  # Берём значение переменной окружения
        if value is not None:  # Проверяем, что значение есть
            try:  # Пробуем преобразовать к целому числу
                return int(value)  # Возвращаем успешно сконвертированное число
            except Exception:  # При ошибке разбора
                continue  # Пропускаем значение и проверяем следующий ключ
    return default  # Если ни один ключ не подошёл, отдаём значение по умолчанию


def _env_float(*keys: str, default: float = 0.0) -> float:  # Добавляем парсер чисел с плавающей точкой
    """Return the first present float environment variable."""  # Поясняем назначение функции

    for key in keys:  # Идём по всем кандидатам
        value = os.getenv(key)  # Считываем значение переменной окружения
        if value is not None:  # Убеждаемся, что значение существует
            try:  # Пробуем перевести его во float
                return float(value)  # Возвращаем результат преобразования
            except Exception:  # Обрабатываем нечисловой ввод
                continue  # Переходим к следующему ключу при ошибке
    return default  # При отсутствии подходящих значений возвращаем дефолт


_REPORT_TZ_RAW = os.getenv("REPORT_TZ", "Europe/Moscow")  # Берём исходный часовой пояс
REPORT_TZ = _REPORT_TZ_RAW.strip() or "Europe/Moscow"  # Обеспечиваем непустой код пояса
try:
    REPORT_TZINFO = ZoneInfo(REPORT_TZ)  # Создаём объект ZoneInfo для дат
except Exception:  # pragma: no cover - защитный путь при неверном поясе
    REPORT_TZ = "Europe/Moscow"  # Сбрасываемся на безопасный часовой пояс при ошибке
    REPORT_TZINFO = ZoneInfo(REPORT_TZ)  # Переинициализируем таймзону после отката

try:  # Предоставляем pytz-совместимый объект для старых участков кода
    TIMEZONE = pytz.timezone(REPORT_TZ)  # Создаём pytz-таймзону по актуальному коду пояса
except Exception:  # pragma: no cover - fallback при ошибке внутри pytz
    TIMEZONE = pytz.timezone("Europe/Moscow")  # Возвращаемся к безопасному pytz-поясу при сбое

HISTORY_DB = (  # Главная база истории отправок
    os.getenv("HISTORY_DB", "var/send_history.db").strip() or "var/send_history.db"
)  # Берём путь из окружения либо используем дефолт
SENT_LOG_PATH = (  # Основной CSV с отправками
    os.getenv("SENT_LOG_PATH", "var/sent_log.csv").strip() or "var/sent_log.csv"
)  # Возвращаем путь к sent_log.csv или значение из окружения
AUDIT_DIR = os.getenv("AUDIT_DIR", "var").strip() or "var"  # Каталог с аудит-логами


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
)  # Завершаем получение строки user-agent для краулера
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
PDF_ENGINE = _str("EMAILBOT_PDF_ENGINE", "fitz")  # Выбираем движок PDF из окружения
PDF_MAX_PAGES = rc_get(  # Настраиваем лимит страниц при прямом чтении PDF
    "PDF_MAX_PAGES",  # Определяем ключ runtime-конфига
    _env_int("PDF_MAX_PAGES", "PDF_PAGE_LIMIT", default=40),  # Поддерживаем альтернативные имена переменной окружения
)  # Задаём лимит страниц по умолчанию
# Фиксированный таймаут остаётся как резервный (если адаптивный выключен)
PDF_EXTRACT_TIMEOUT = rc_get(
    "PDF_EXTRACT_TIMEOUT", _int("PDF_EXTRACT_TIMEOUT", 25)
)  # seconds
PDF_PAGE_TIMEOUT_SEC = rc_get(  # Ограничиваем длительность извлечения текста для одной страницы
    "PDF_PAGE_TIMEOUT_SEC",  # Берём значение из runtime-конфига
    _env_float("PDF_PAGE_TIMEOUT_SEC", default=8.0),  # Читаем дефолт из окружения с плавающей точкой
)  # Сохраняем тайм-аут страницы PDF в секундах
PDF_FAST_LIMIT_PAGES = rc_get(  # Вводим мягкий предел страниц для быстрого профиля
    "PDF_FAST_LIMIT_PAGES",  # Регистрируем ключ в runtime-конфиге
    _env_int("PDF_FAST_LIMIT_PAGES", default=200),  # Используем целочисленный дефолт из окружения
)  # Фиксируем лимит страниц в быстром профиле
PDF_PROFILE = rc_get(  # Определяем активный профиль обработки PDF
    "PDF_PROFILE",  # Обращаемся к runtime-конфига по ключу профиля
    (_str("PDF_PROFILE", "fast") or "fast").strip().lower(),  # Нормализуем строку из окружения с дефолтом fast
)  # Храним название выбранного профиля
if PDF_PROFILE not in {"fast", "full"}:  # Проверяем корректность указанного профиля
    PDF_PROFILE = "fast"  # Возвращаемся к безопасному профилю fast при ошибке
PDF_FOUND_TARGET = rc_get(  # Определяем число адресов, достаточное для раннего выхода
    "PDF_FOUND_TARGET",  # Считываем ключ из runtime-конфига
    _env_int("PDF_FOUND_TARGET", default=20),  # Поддерживаем настройку через переменные окружения
)  # Сохраняем целевой порог найденных адресов
PDF_SCAN_SAMPLE = rc_get(  # Управляем включением выборочного сканирования
    "PDF_SCAN_SAMPLE",  # Используем ключ для runtime-конфига
    _env_int("PDF_SCAN_SAMPLE", default=1),  # Читаем флаг выборочного обхода из окружения
)  # Храним индикатор выборочного сканирования страниц
PDF_SCAN_STRIDE = rc_get(  # Определяем шаг выборочного сканирования
    "PDF_SCAN_STRIDE",  # Запоминаем ключ в runtime-конфиге
    max(1, _env_int("PDF_SCAN_STRIDE", default=5)),  # Гарантируем положительный шаг, считанный из окружения
)  # Сохраняем шаг через количество страниц
PDF_STOP_AFTER_NO_HITS = rc_get(  # Настраиваем лимит последовательных пустых страниц
    "PDF_STOP_AFTER_NO_HITS",  # Объявляем ключ runtime-конфига
    _env_int("PDF_STOP_AFTER_NO_HITS", default=12),  # Берём дефолт из переменной окружения
)  # Завершаем определение порога остановки по отсутствию находок
_ENABLE_OCR_DEFAULT = _env_bool(  # Формируем дефолт для OCR из нескольких переменных
    "ENABLE_OCR",  # Учитываем новое общее имя флага
    "EMAILBOT_ENABLE_OCR",  # Сохраняем поддержку старого флага бота
    "PDF_OCR_AUTO",  # Добавляем исторический ключ автоматического OCR
    default=_int("PDF_OCR_AUTO", 1),  # Падаем назад на старый дефолт из окружения
)  # Завершаем сбор значений по всем совместимым ключам OCR
EMAILBOT_ENABLE_OCR = rc_get(  # Обновляем флаг включения OCR в runtime-конфиге
    "EMAILBOT_ENABLE_OCR",  # Используем старое имя ключа для совместимости
    bool(_ENABLE_OCR_DEFAULT),  # Преобразуем числовой флаг в булево значение
)  # Фиксируем булевый признак включённого OCR в runtime-конфиге
ENABLE_OCR = rc_get(  # Даём новое имя параметру для явного включения OCR
    "ENABLE_OCR",  # Фиксируем ключ для runtime-конфига
    EMAILBOT_ENABLE_OCR,  # Переносим вычисленное значение по умолчанию
)  # Объявляем результирующий флаг ENABLE_OCR с учётом runtime-конфига
# -------- PDF / OCR Auto Mode --------
PDF_OCR_AUTO = rc_get(  # Загружаем режим автоматического выбора OCR
    "PDF_OCR_AUTO",  # Используем ключ PDF_OCR_AUTO в runtime-конфиге
    _ENABLE_OCR_DEFAULT,  # Передаём дефолт, совместимый с новыми переменными окружения
)  # Подтверждаем итоговый режим автоматического OCR
PDF_OCR_PROBE_PAGES = rc_get(  # Настраиваем количество страниц для первичной проверки текста
    "PDF_OCR_PROBE_PAGES",  # Обращаемся к runtime-конфигу по соответствующему ключу
    _env_int("PDF_OCR_PROBE_PAGES", "OCR_PROBE_PAGES", default=5),  # Поддерживаем альтернативное имя переменной
)  # Сохраняем лимит страниц для быстрого прогона перед OCR
PDF_OCR_MAX_PAGES = rc_get(  # Определяем максимум страниц, которые обрабатываем через OCR
    "PDF_OCR_MAX_PAGES",  # Считываем ключ из runtime-конфига
    _env_int("PDF_OCR_MAX_PAGES", "OCR_MAX_PAGES", default=300),  # Учитываем новое и старое имя переменной окружения
)  # Подтверждаем итоговый лимит страниц для OCR-прохода
PDF_OCR_MIN_TEXT_RATIO = rc_get(  # Устанавливаем минимальную долю текстовых страниц
    "PDF_OCR_MIN_TEXT_RATIO",  # Забираем значение из runtime-конфига
    _env_float("PDF_OCR_MIN_TEXT_RATIO", "OCR_MIN_TEXT_RATIO", default=0.05),  # Берём дефолт из совместимого окружения
)  # Завершаем чтение порога доли текстовых страниц в документе
PDF_OCR_MIN_CHARS = rc_get(  # Настраиваем минимальное количество символов на странице
    "PDF_OCR_MIN_CHARS",  # Читаем ключ из runtime-конфига
    _env_int("PDF_OCR_MIN_CHARS", "OCR_MIN_CHARS", default=150),  # Обрабатываем оба имени переменной окружения
)  # Подтверждаем порог минимального количества символов на страницу
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "").strip()  # Сохраняем исходный путь к Tesseract
OCR_TESSERACT_CMD = rc_get(  # Переэкспортируем путь к tesseract под новым именем
    "OCR_TESSERACT_CMD",  # Используем явный ключ в runtime-конфиге
    _env_str("OCR_TESSERACT_CMD", "TESSERACT_CMD", default=TESSERACT_CMD),  # Поддерживаем оба варианта переменной окружения
)  # Завершаем экспорт пути к tesseract в runtime-конфиг

# -------- Ранний прогресс / тёплый старт --------
PDF_WARMUP_PAGES = rc_get(  # Берём число страниц для быстрого прогрева, по умолчанию четыре
    "PDF_WARMUP_PAGES",  # Передаём ключ параметра, чтобы можно было переопределить в runtime
    _int("PDF_WARMUP_PAGES", 4),  # Считываем значение из окружения с дефолтом 4 страницы
)
PDF_EARLY_HEARTBEAT_SEC = rc_get(  # Настраиваем интервал молчания перед ранним heartbeat
    "PDF_EARLY_HEARTBEAT_SEC",  # Используем читаемый ключ для runtime-конфига
    _float("PDF_EARLY_HEARTBEAT_SEC", 2.5),  # Берём значение из окружения, дефолт 2.5 секунды
)
PDF_WARMUP_MIN_FOUND = rc_get(  # Фиксируем минимальное количество email после warmup
    "PDF_WARMUP_MIN_FOUND",  # Объявляем ключ для динамического управления
    _int("PDF_WARMUP_MIN_FOUND", 1),  # Считываем порог из окружения, по умолчанию хотя бы один адрес
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

PDF_OCR_ENGINE = rc_get(  # Определяем движок OCR с поддержкой альтернативных имён
    "PDF_OCR_ENGINE",  # Ключ runtime-конфига для движка OCR
    _str("PDF_OCR_ENGINE", _str("OCR_ENGINE", "pytesseract")),  # Объединяем новые и старые переменные окружения
)  # Завершаем конфигурацию движка OCR с учётом совместимости
PDF_OCR_LANG = rc_get(  # Указываем языки OCR с учётом совместимости
    "PDF_OCR_LANG",  # Используем ключ runtime-конфига для списка языков
    _str("PDF_OCR_LANG", _str("OCR_LANG", "eng+rus")),  # Считаем значения из всех подходящих переменных окружения
)  # Фиксируем набор языков OCR в runtime-конфиге
OCR_ENGINE = rc_get(  # Предоставляем сокращённое имя движка OCR
    "OCR_ENGINE",  # Сохраняем ключ в runtime-конфиге
    PDF_OCR_ENGINE,  # Переносим рассчитанный ранее дефолт
)  # Предоставляем синонимичный доступ к движку OCR
OCR_LANG = rc_get(  # Предоставляем сокращённое имя списка языков
    "OCR_LANG",  # Регистрируем ключ в runtime-конфиге
    PDF_OCR_LANG,  # Используем базовое значение из совместимого блока
)  # Экспортируем языковой набор OCR под укороченным именем
PDF_OCR_PAGE_LIMIT = rc_get(  # Контролируем лимит страниц в OCR-потоке
    "PDF_OCR_PAGE_LIMIT",  # Читаем ключ runtime-конфига
    _env_int(  # Сливаем несколько переменных окружения
        "PDF_OCR_PAGE_LIMIT",  # Основное имя параметра
        "OCR_PAGE_LIMIT",  # Альтернативное имя окружения
        default=PDF_OCR_MAX_PAGES if PDF_OCR_MAX_PAGES > 0 else 10,  # Используем дефолт, согласованный с общим лимитом
    ),
)  # Утверждаем лимит страниц, которые разрешено гонять через OCR
PDF_OCR_TIME_LIMIT = rc_get(  # Устанавливаем общий тайм-аут на операцию OCR
    "PDF_OCR_TIME_LIMIT",  # Берём ключ runtime-конфига
    _env_int("PDF_OCR_TIME_LIMIT", "OCR_TIME_LIMIT", default=30),  # Поддерживаем старые и новые имена переменных
)  # Фиксируем суммарный тайм-аут выполнения OCR
PDF_OCR_TIMEOUT_PER_PAGE = rc_get(  # Настраиваем тайм-аут на страницу при OCR
    "PDF_OCR_TIMEOUT_PER_PAGE",  # Читаем ключ runtime-конфига
    _env_int("PDF_OCR_TIMEOUT_PER_PAGE", "OCR_TIMEOUT_PER_PAGE", default=20),  # Сводим все совместимые переменные окружения
)  # Устанавливаем бюджет времени на обработку одной страницы
PDF_OCR_DPI = rc_get(  # Определяем DPI рендеринга страниц для OCR
    "PDF_OCR_DPI",  # Используем ключ runtime-конфига
    _env_int("PDF_OCR_DPI", "OCR_DPI", default=300),  # Принимаем значения из нескольких переменных окружения
)  # Подтверждаем разрешение растеризации страницы перед OCR
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
