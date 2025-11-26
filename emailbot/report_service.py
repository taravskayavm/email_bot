"""Helpers for building delivery reports."""  # Описание модуля

from __future__ import annotations  # Поддержка аннотаций будущих версий

import csv  # Чтение CSV-файлов с журналами
import datetime as dt  # Работа с датой и временем
import json  # Обработка формата JSON
import os  # Работа с переменными окружения и путями
import sys  # Проверка версии интерпретатора
from itertools import chain  # Объединение нескольких итераторов
from pathlib import Path  # Тип для описания путей
from typing import Iterator  # Аннотация для итераторов

from emailbot.messaging import LOG_FILE  # Путь к журналу из модуля отправки сообщений
from utils import send_stats as _send_stats  # Доступ к вспомогательной функции статистики

if sys.version_info >= (3, 9):  # Проверяем наличие стандартного ZoneInfo
    from zoneinfo import ZoneInfo  # Используем стандартную реализацию
else:  # pragma: no cover - legacy fallback for Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore  # Запасной вариант для старых Python


def _resolve_sent_log_path() -> Path:
    """Resolve path to ``sent_log.csv`` compatible with messaging module."""  # Докстринг функции

    env = os.getenv("SENT_LOG_PATH")  # Получаем путь из переменной окружения, если установлен
    if env:  # Проверяем, предоставил ли пользователь значение
        try:  # Пробуем корректно развернуть путь
            return Path(os.path.expanduser(env)).resolve()  # Возвращаем абсолютный путь после expanduser
        except Exception:  # На случай нестандартных данных пути
            return Path(env).expanduser().resolve()  # Пытаемся привести путь к форме Path с расширением
    return Path(os.path.expanduser(LOG_FILE)).resolve()  # Используем путь из messaging как значение по умолчанию


def _resolve_send_stats_path() -> Path:
    """Resolve path to ``send_stats.jsonl`` in the same way as :mod:`utils.send_stats`."""  # Докстринг функции

    try:  # Используем внутренний хелпер для получения пути, если доступен
        return _send_stats._stats_path()  # type: ignore[attr-defined]  # Берем путь через приватную функцию
    except Exception:  # Отлавливаем любые ошибки, чтобы не нарушить отчеты
        raw = os.getenv("SEND_STATS_PATH", "var/send_stats.jsonl")  # Подхватываем путь из окружения или дефолт
        expanded = os.path.expandvars(os.path.expanduser(str(raw)))  # Раскрываем переменные окружения и тильду
        return Path(expanded)  # Возвращаем итоговый Path


SENT_LOG_PATH = _resolve_sent_log_path()  # Итоговый путь к журналу отправок
SEND_STATS_PATH = _resolve_send_stats_path()  # Итоговый путь к файлу статистики
REPORT_TZ_NAME = (os.getenv("REPORT_TZ") or "Europe/Moscow").strip() or "Europe/Moscow"  # Имя часового пояса
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)  # Объект часового пояса для преобразований

_SUCCESS_STATUSES = {"sent", "success", "ok", "synced"}  # Набор статусов успешной отправки
_ERROR_STATUSES = {  # Набор статусов ошибок
    "failed",  # Общее обозначение ошибки
    "fail",  # Короткое обозначение ошибки
    "error",  # Прямое указание ошибки
    "bounce",  # Отказ доставки
    "bounced",  # Отказ доставки в прошедшем времени
    "soft_bounce",  # Временный отказ доставки
    "soft-bounce",  # Альтернативное написание временного отказа
    "hard_bounce",  # Жесткий отказ доставки
    "hard-bounce",  # Альтернативное написание жесткого отказа
}  # Завершение набора ошибочных статусов


def _parse_ts_any(raw: str) -> dt.datetime | None:
    """Parse various ISO8601 timestamps and normalise them to UTC."""  # Докстринг функции

    value = raw.strip()  # Удаляем лишние пробелы
    if not value:  # Если строка пустая
        return None  # Возвращаем отсутствие результата
    normalized = value.replace("Z", "+00:00")  # Заменяем Z на смещение для совместимости fromisoformat
    try:  # Пытаемся распарсить дату
        ts = dt.datetime.fromisoformat(normalized)  # Получаем datetime из строки
    except ValueError:  # Если формат не поддерживается
        return None  # Возвращаем отсутствие результата
    if ts.tzinfo is None:  # Если информация о часовом поясе отсутствует
        ts = ts.replace(tzinfo=dt.timezone.utc)  # Предполагаем UTC
    else:  # Если часовой пояс присутствует
        ts = ts.astimezone(dt.timezone.utc)  # Переводим в UTC для унификации
    return ts  # Возвращаем итоговый объект времени


def _detect_delimiter(sample: str) -> str:
    """Return the more frequent delimiter between ``;`` and ``,``."""  # Докстринг функции

    semi = sample.count(";")  # Считаем количество точек с запятой
    comma = sample.count(",")  # Считаем количество запятых
    return ";" if semi > comma else ","  # Возвращаем наиболее частый разделитель


def _iter_sent_log() -> Iterator[dict[str, object]]:
    """Yield records from ``sent_log.csv`` regardless of schema version."""  # Докстринг функции

    path = SENT_LOG_PATH  # Получаем путь к журналу
    if not path.exists():  # Если файла нет
        return  # Прерываем генератор

    try:  # Пытаемся открыть файл
        with path.open("r", encoding="utf-8", newline="") as handle:  # Читаем файл с явной кодировкой
            sample = handle.read(1024)  # Считываем пример для определения разделителя
            handle.seek(0)  # Возвращаемся в начало файла
            delimiter = _detect_delimiter(sample)  # Выбираем подходящий разделитель
            reader = csv.DictReader(handle, delimiter=delimiter)  # Создаем словарный CSV-читатель
            for row in reader:  # Итерируемся по строкам
                if not row:  # Если строка пустая
                    continue  # Пропускаем её
                ts_raw = (row.get("last_sent_at") or row.get("ts") or "").strip()  # Берем timestamp из возможных полей
                ts_utc = _parse_ts_any(ts_raw)  # Преобразуем строку в datetime UTC
                if not ts_utc:  # Если дата не разобрана
                    continue  # Пропускаем запись
                status_raw = (row.get("status") or "").strip()  # Получаем статус из строки
                status = status_raw.lower() if status_raw else "sent"  # Нормализуем статус
                yield {"ts_utc": ts_utc, "status": status}  # Возвращаем словарь с временем и статусом
    except FileNotFoundError:  # Если файл исчез после проверки
        return  # Завершаем функцию


def _iter_send_stats() -> Iterator[dict[str, object]]:
    """Yield records from ``send_stats.jsonl`` if available."""  # Докстринг функции

    path = SEND_STATS_PATH  # Получаем путь к файлу статистики
    if not path.exists():  # Проверяем существование файла
        return  # Завершаем итератор

    with path.open("r", encoding="utf-8") as handle:  # Открываем файл в текстовом режиме
        for line in handle:  # Проходим по строкам
            payload = line.strip()  # Убираем пробелы и перевод строки
            if not payload:  # Если строка пустая
                continue  # Пропускаем
            try:  # Пытаемся разобрать JSON
                data = json.loads(payload)  # Преобразуем строку в словарь
            except json.JSONDecodeError:  # Если формат невалиден
                continue  # Пропускаем строку
            ts_raw = str(data.get("ts") or "").strip()  # Получаем исходный timestamp
            ts_utc = _parse_ts_any(ts_raw)  # Преобразуем timestamp в datetime UTC
            if not ts_utc:  # Если дата не разобрана
                continue  # Пропускаем
            status_raw = data.get("status")  # Пытаемся извлечь статус
            if status_raw:  # Если статус задан
                status = str(status_raw).strip().lower()  # Нормализуем статус
            else:  # Если статус отсутствует
                status = "sent" if data.get("success") else "failed"  # Определяем статус по признаку успеха
            yield {"ts_utc": ts_utc, "status": status}  # Возвращаем запись со временем и статусом



def summarize_day_local(today_local: dt.date | None = None) -> tuple[int, int]:
    """Return counts of successful and failed deliveries for the local day."""  # Докстринг функции

    if today_local is None:  # Если дата не передана
        today_local = dt.datetime.now(REPORT_TZ).date()  # Берем текущую дату в отчетном поясе
    start_local = dt.datetime.combine(today_local, dt.time(0, 0, 0), tzinfo=REPORT_TZ)  # Начало дня
    end_local = start_local + dt.timedelta(days=1)  # Конец дня

    ok = 0  # Счетчик успешных отправок
    err = 0  # Счетчик ошибок
    for item in chain(_iter_sent_log(), _iter_send_stats()):  # Объединяем источники данных
        ts_utc = item["ts_utc"]  # Получаем время отправки
        if not isinstance(ts_utc, dt.datetime):  # Проверяем тип значения
            continue  # Пропускаем некорректные записи
        status = str(item.get("status", "")).strip().lower()  # Нормализуем статус
        ts_local = ts_utc.astimezone(REPORT_TZ)  # Переводим время в локальный пояс
        if not (start_local <= ts_local < end_local):  # Фильтруем записи за текущий день
            continue  # Пропускаем записи вне диапазона
        if status in _SUCCESS_STATUSES:  # Если статус успешный
            ok += 1  # Увеличиваем счетчик успехов
        elif status in _ERROR_STATUSES:  # Если статус ошибочный
            err += 1  # Увеличиваем счетчик ошибок
    return ok, err  # Возвращаем итоговые подсчеты


__all__ = [  # Список публичных объектов
    "REPORT_TZ",  # Экспортируем часовой пояс
    "REPORT_TZ_NAME",  # Экспортируем имя часового пояса
    "SENT_LOG_PATH",  # Экспортируем путь к журналу отправок
    "SEND_STATS_PATH",  # Экспортируем путь к файлу статистики
    "summarize_day_local",  # Экспортируем основную функцию
]  # Конец списка экспорта
