"""Helpers for building delivery reports."""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sys
from itertools import chain
from pathlib import Path
from typing import Iterator

from emailbot.suppress_list import blocklist_path
from . import reporting  # Модуль агрегации метрик для повторного использования.
from utils import send_stats

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:  # pragma: no cover - legacy fallback for Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore


def _resolve_sent_log_path() -> Path:
    """Resolve path to ``sent_log.csv`` in the shared data directory."""

    env = os.getenv("SENT_LOG_PATH")  # Получаем путь из переменной окружения, если указан.
    if env:
        expanded = os.path.expanduser(env)  # Разворачиваем ``~`` до домашней директории.
        expanded = os.path.expandvars(expanded)  # Подставляем переменные окружения в пути.
        return Path(expanded).resolve()  # Возвращаем абсолютный путь к файлу лога.

    data_dir = blocklist_path().parent  # Определяем общую директорию с блоклистом.
    return (data_dir / "sent_log.csv").resolve()  # Размещаем лог рядом с ``blocked_emails.txt``.


def _resolve_send_stats_path() -> Path:
    """Resolve path to ``send_stats.jsonl`` the same way as :mod:`utils.send_stats`."""

    return send_stats._stats_path()  # Используем путь, который задаёт ``utils.send_stats``.


SENT_LOG_PATH = _resolve_sent_log_path()  # Финальный путь для файла ``sent_log.csv``.
SEND_STATS_PATH = _resolve_send_stats_path()  # Финальный путь для файла ``send_stats.jsonl``.
REPORT_TZ_RAW = os.getenv("REPORT_TZ")  # Получаем значение часового пояса из окружения.
# Часовой пояс отчёта, с запасным значением по умолчанию.
REPORT_TZ_NAME = (REPORT_TZ_RAW or "Europe/Moscow").strip() or "Europe/Moscow"
# Объект таймзоны для локализации времени.
REPORT_TZ = ZoneInfo(REPORT_TZ_NAME)

_SUCCESS_STATUSES = {"sent", "success", "ok", "synced"}
_ERROR_STATUSES = {
    "failed",
    "fail",
    "error",
    "bounce",
    "bounced",
    "soft_bounce",
    "soft-bounce",
    "hard_bounce",
    "hard-bounce",
}


def _parse_ts_any(raw: str) -> dt.datetime | None:
    """Parse various ISO8601 timestamps and normalise them to UTC."""

    value = raw.strip()
    if not value:
        return None
    # Replace trailing "Z" to maintain compatibility with ``fromisoformat``
    normalized = value.replace("Z", "+00:00")
    try:
        ts = dt.datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts = ts.astimezone(dt.timezone.utc)
    return ts


def _detect_delimiter(sample: str) -> str:
    semi = sample.count(";")
    comma = sample.count(",")
    return ";" if semi > comma else ","


def _iter_sent_log() -> Iterator[dict[str, object]]:
    """Yield records from ``sent_log.csv`` regardless of schema version."""

    path = SENT_LOG_PATH
    if not path.exists():
        return

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            sample = handle.read(1024)
            handle.seek(0)
            delimiter = _detect_delimiter(sample)
            reader = csv.DictReader(handle, delimiter=delimiter)
            for row in reader:
                if not row:
                    continue
                ts_raw = (row.get("last_sent_at") or row.get("ts") or "").strip()
                ts_utc = _parse_ts_any(ts_raw)
                if not ts_utc:
                    continue
                status_raw = (row.get("status") or "").strip()
                status = status_raw.lower() if status_raw else "sent"
                yield {"ts_utc": ts_utc, "status": status}
    except FileNotFoundError:
        return


def _iter_send_stats() -> Iterator[dict[str, object]]:
    """Yield records from ``send_stats.jsonl`` if available."""

    path = SEND_STATS_PATH
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = line.strip()
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            ts_raw = str(data.get("ts") or "").strip()
            ts_utc = _parse_ts_any(ts_raw)
            if not ts_utc:
                continue
            status_raw = data.get("status")
            if status_raw:
                status = str(status_raw).strip().lower()
            else:
                status = "sent" if data.get("success") else "failed"
            yield {"ts_utc": ts_utc, "status": status}



def summarize_day_local(today_local: dt.date | None = None) -> tuple[int, int]:
    """
    Return counts of successful and failed deliveries for the local day.

    Аргумент ``today_local`` сохранён для обратной совместимости и в текущей
    реализации не используется напрямую: фактический подсчёт делегируется
    функции ``emailbot.reporting.summarize_period_stats("day")``, которая
    агрегирует статистику по всем направлениям за текущие сутки.
    """

    # Суммарная статистика за сутки по всем источникам.
    period_stats = reporting.summarize_period_stats("day")
    # Количество успешных отправок за период.
    ok = period_stats.total_success
    # Количество неудачных отправок за период.
    err = period_stats.total_failed
    # Возвращаем кортеж (успехи, ошибки) для совместимости.
    return ok, err


__all__ = [
    "REPORT_TZ",
    "REPORT_TZ_NAME",
    "SENT_LOG_PATH",
    "SEND_STATS_PATH",
    "summarize_day_local",
]
