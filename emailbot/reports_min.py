"""Минимальные отчёты по отправкам за текущий день."""

from __future__ import annotations  # Поддерживаем отложенные аннотации типов

import csv  # Работаем с CSV-логом отправок
from datetime import datetime, time  # Берём инструменты для расчёта границ дня
from pathlib import Path  # Представляем путь к логу как Path
from typing import Tuple  # Описываем тип возвращаемого кортежа

from utils.paths import expand_path  # Расширяем относительные пути до абсолютных

from .config import REPORT_TZINFO, SENT_LOG_PATH  # Берём TZ и путь к CSV


def _day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
    """Вернуть границы суток в часовом поясе отчётов для даты ``dt``."""

    tz = REPORT_TZINFO  # Берём объект таймзоны отчёта
    start = datetime.combine(dt.date(), time(0, 0, 0), tzinfo=tz)  # Начало суток
    end = datetime.combine(dt.date(), time(23, 59, 59), tzinfo=tz)  # Конец суток
    return start, end  # Возвращаем границы периода


def _parse_ts(raw: str | None) -> datetime | None:
    """Попробовать преобразовать ISO-строку ``raw`` в datetime."""

    if not raw:  # Пропускаем отсутствующие значения
        return None  # Нет времени — нечего возвращать
    cleaned = raw.strip()  # Убираем пробелы по краям
    if not cleaned:  # Проверяем, что строка не стала пустой
        return None  # Возвращаем None, если данных нет
    normalized = cleaned.replace("Z", "+00:00")  # Поддерживаем UTC-суффикс Z
    try:
        return datetime.fromisoformat(normalized)  # Используем стандартный парсер ISO
    except ValueError:
        return None  # При ошибке парсинга запись пропускаем


def build_minimal_summary_for_today() -> tuple[int, int, int]:
    """Вернуть числа отправленных, заблокированных и ошибочных писем за сегодня."""

    now = datetime.now(REPORT_TZINFO)  # Берём текущее время в нужной таймзоне
    start, end = _day_bounds(now)  # Рассчитываем границы сегодняшних суток
    sent = blocked = error = 0  # Инициализируем счётчики категорий
    log_path = expand_path(Path(SENT_LOG_PATH))  # Приводим путь к файлу лога
    if not log_path.exists():  # Если файл не найден, возвращаем нулевую статистику
        return sent, blocked, error  # Пустая сводка при отсутствии данных
    with log_path.open("r", encoding="utf-8", newline="") as handle:  # Читаем CSV файл
        reader = csv.DictReader(handle)  # Читаем строки по именам колонок
        for row in reader:  # Обрабатываем каждую строку лога
            ts = _parse_ts(row.get("timestamp"))  # Пытаемся прочитать время события
            if ts is None:  # Если время не распознано, пропускаем запись
                continue  # Строка без времени не попадает в отчёт
            if ts.tzinfo is None:  # Проверяем, указан ли часовой пояс
                ts = ts.replace(tzinfo=REPORT_TZINFO)  # Назначаем TZ отчёта
            else:
                ts = ts.astimezone(REPORT_TZINFO)  # Переводим время в нужную таймзону
            if not (start <= ts <= end):  # Фильтруем события вне текущего дня
                continue  # Пропускаем события за другие сутки
            status = (row.get("status") or "").strip().lower()  # Нормализуем статус
            if status == "sent":  # Фиксируем успешную отправку
                sent += 1  # Увеличиваем счётчик успешных писем
            elif status == "blocked":  # Фиксируем блокировку
                blocked += 1  # Увеличиваем счётчик блокировок
            elif status == "error":  # Фиксируем ошибку доставки
                error += 1  # Увеличиваем счётчик ошибок
            else:
                continue  # Неизвестные статусы игнорируем
    return sent, blocked, error  # Возвращаем итоговые показатели
