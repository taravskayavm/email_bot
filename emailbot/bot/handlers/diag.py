"""Handlers that provide runtime diagnostics for the Telegram email bot."""

# Обеспечиваем совместимость аннотаций с будущим поведением Python.
from __future__ import annotations

# Импортируем os для чтения переменных окружения.
import os

# Импортируем Router для регистрации обработчиков.
from aiogram import Router
# Импортируем фильтр Command для обработки команд.
from aiogram.filters import Command
# Импортируем тип Message для ответов пользователю.
from aiogram.types import Message

# Создаем экземпляр роутера для регистрации локальных хендлеров.
router = Router()


def _bool_env(name: str, default: bool = False) -> bool:
    """Преобразует значение переменной окружения в булево представление."""

    # Получаем исходное значение переменной окружения.
    raw = os.getenv(name)
    # Проверяем, задана ли переменная окружения.
    if raw is None:
        # Возвращаем значение по умолчанию, если переменная отсутствует.
        return default
    # Сравниваем нормализованное значение с истинными маркерами.
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env(name: str, default: str = "") -> str:
    """Возвращает строковое значение переменной окружения с запасным значением."""

    # Читаем переменную окружения с подстановкой значения по умолчанию.
    return os.getenv(name, default)


# Регистрируем обработчик для команд /diag и /diagnostics.
@router.message(Command("diag", "diagnostics"))
async def cmd_diag(message: Message) -> None:
    """Отправляет диагностическую информацию по конфигурации почтового бота."""

    # Получаем имя SMTP-хоста из окружения.
    smtp_host = _env("SMTP_HOST")
    # Получаем порт SMTP из окружения.
    smtp_port = _env("SMTP_PORT")
    # Получаем режим SMTP или используем "auto" по умолчанию.
    smtp_mode = _env("SMTP_MODE", "auto")
    # Определяем включенный SSL-режим для SMTP.
    smtp_ssl = _env("SMTP_SSL")
    # Читаем таймаут SMTP, если задан.
    smtp_timeout = _env("SMTP_TIMEOUT")

    # Получаем имя IMAP-хоста.
    imap_host = _env("IMAP_HOST")
    # Получаем порт IMAP.
    imap_port = _env("IMAP_PORT")
    # Получаем таймаут IMAP, если указан.
    imap_timeout = _env("IMAP_TIMEOUT")

    # Читаем адрес электронной почты для учетной записи.
    email_address = _env("EMAIL_ADDRESS")

    # Получаем дневной лимит отправки писем.
    daily_send_limit = _env("DAILY_SEND_LIMIT")
    # Получаем максимальное число писем в сутки.
    max_emails_per_day = _env("MAX_EMAILS_PER_DAY")
    # Получаем длительность периода охлаждения.
    cooldown_days = _env("COOLDOWN_DAYS")

    # Читаем период анализа исходящих писем.
    email_lookback_days = _env("EMAIL_LOOKBACK_DAYS")
    # Получаем интервал синхронизации почты.
    email_sync_interval_hours = _env("EMAIL_SYNC_INTERVAL_HOURS")

    # Проверяем, включен ли watchdog.
    watchdog_enabled = _bool_env("EMAILBOT_WATCHDOG_ENABLED", False)
    # Узнаем, принудительно ли отменяются задачи.
    watchdog_enforce_cancel = _bool_env("EMAILBOT_WATCHDOG_ENFORCE_CANCEL", False)
    # Получаем таймаут watchdog в секундах.
    watchdog_timeout = _env("EMAILBOT_WATCHDOG_TIMEOUT_SEC")

    # Проверяем, включен ли диагностический пинг SMTP.
    diag_ping_smtp = _bool_env("DIAG_PING_SMTP", False)
    # Проверяем, включен ли диагностический пинг IMAP.
    diag_ping_imap = _bool_env("DIAG_PING_IMAP", False)

    # Инициализируем список строк для человекочитаемого отчета.
    lines: list[str] = []
    # Добавляем заголовок диагностики.
    lines.append("Диагностика e-mail бота:")
    # Добавляем пустую строку для визуального разделения.
    lines.append("")
    # Добавляем заголовок секции SMTP.
    lines.append("SMTP:")
    # Отображаем хост SMTP или дефолтный символ.
    lines.append(f"  host: {smtp_host or '-'}")
    # Отображаем порт SMTP.
    lines.append(f"  port: {smtp_port or '-'}")
    # Показываем режим SMTP и состояние SSL.
    lines.append(f"  mode: {smtp_mode} (SSL={smtp_ssl or '-'})")
    # Показываем таймаут SMTP в секундах.
    lines.append(f"  timeout: {smtp_timeout or '-'} с")
    # Сообщаем, активен ли пинг SMTP.
    lines.append(f"  DIAG_PING_SMTP: {diag_ping_smtp}")
    # Разделяем секции пустой строкой.
    lines.append("")
    # Заголовок секции IMAP.
    lines.append("IMAP:")
    # Отображаем IMAP-хост.
    lines.append(f"  host: {imap_host or '-'}")
    # Отображаем IMAP-порт.
    lines.append(f"  port: {imap_port or '-'}")
    # Показываем таймаут IMAP.
    lines.append(f"  timeout: {imap_timeout or '-'} с")
    # Сообщаем статус пинга IMAP.
    lines.append(f"  DIAG_PING_IMAP: {diag_ping_imap}")
    # Пустая строка для читаемости.
    lines.append("")
    # Заголовок секции учетной записи.
    lines.append("Учётная запись:")
    # Отображаем настроенный email-адрес.
    lines.append(f"  EMAIL_ADDRESS: {email_address or '-'}")
    # Пустая строка между секциями.
    lines.append("")
    # Заголовок секции ограничений.
    lines.append("Ограничения отправки:")
    # Показываем дневной лимит отправки.
    lines.append(f"  DAILY_SEND_LIMIT: {daily_send_limit or '-'}")
    # Показываем максимум писем в день.
    lines.append(f"  MAX_EMAILS_PER_DAY: {max_emails_per_day or '-'}")
    # Показываем период охлаждения между письмами.
    lines.append(f"  COOLDOWN_DAYS: {cooldown_days or '-'}")
    # Показываем глубину анализа исходящих писем.
    lines.append(f"  EMAIL_LOOKBACK_DAYS: {email_lookback_days or '-'}")
    # Показываем интервал синхронизации.
    lines.append(f"  EMAIL_SYNC_INTERVAL_HOURS: {email_sync_interval_hours or '-'}")
    # Пустая строка перед блоком watchdog.
    lines.append("")
    # Заголовок секции watchdog.
    lines.append("Watchdog:")
    # Отображаем статус включения watchdog.
    lines.append(f"  EMAILBOT_WATCHDOG_ENABLED: {watchdog_enabled}")
    # Показываем статус принудительной отмены задач.
    lines.append(f"  EMAILBOT_WATCHDOG_ENFORCE_CANCEL: {watchdog_enforce_cancel}")
    # Отображаем таймаут watchdog.
    lines.append(f"  EMAILBOT_WATCHDOG_TIMEOUT_SEC: {watchdog_timeout or '-'}")

    # Отправляем собранный диагностический текст пользователю.
    await message.answer("\n".join(lines))
