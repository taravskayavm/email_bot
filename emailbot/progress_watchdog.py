"""Async heartbeat/watchdog helpers to detect parser hangs."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path  # Работаем с путями для дампов зависших задач
from telegram.ext import Application, ContextTypes  # Интегрируем watchdog в PTB JobQueue

import faulthandler

from emailbot import runtime_progress  # Повторно используем глобальный маркер прогресса
from emailbot import settings  # Читаем пользовательские таймауты и флаги watchdog

__all__ = [  # Чётко объявляем публичные символы, чтобы облегчить импорт вызывающим модулям
    "heartbeat",  # Экспортируем корутину обновления отметки из async-контекста
    "heartbeat_now",  # Оставляем синхронную версию для to_thread/IO-кода
    "start_watchdog",  # Позволяем запускать сторож для отдельных задач
    "start_heartbeat_pulse",  # Отдаём вспомогательную корутину периодического heartbeat
    "touch",  # Новая обёртка для глобального прогресса
    "last_touch",  # Экспонируем чтение таймштампа глобального прогресса
    "install",  # Добавляем функцию подключения watchdog к PTB JobQueue
]


def touch(reason: str = "heartbeat") -> None:  # Объявляем обёртку для обновления прогресса
    """Проксируем обновление прогресса в общий потокобезопасный маркер."""

    runtime_progress.touch(reason)  # Доверяем глобальному helper'у вести таймштамп


def last_touch() -> float:  # Объявляем функцию для получения момента последней активности
    """Возвращаем время последней активности, зафиксированной runtime_progress."""

    return runtime_progress.last_touch()  # Забираем сохранённый таймштамп без дублирования состояния

_last_beat: float = 0.0
_lock = asyncio.Lock()


async def heartbeat() -> None:
    """Record a heartbeat timestamp from async code."""

    global _last_beat
    async with _lock:
        _last_beat = time.monotonic()


def heartbeat_now() -> None:
    """Record a heartbeat timestamp from synchronous/to_thread code."""

    global _last_beat
    _last_beat = time.monotonic()


async def _heartbeat_pulse(interval: float = 5.0) -> None:
    """Emit periodic heartbeats while awaiting long-running operations."""

    try:
        while True:
            await asyncio.sleep(max(interval, 0.1))
            await heartbeat()
    except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
        pass


def start_heartbeat_pulse(*, interval: float = 5.0) -> asyncio.Task[None]:
    """Start a background task that periodically emits heartbeats."""

    loop = asyncio.get_running_loop()
    return loop.create_task(_heartbeat_pulse(interval))


async def start_watchdog(
    task: asyncio.Task[object],
    *,
    idle_seconds: float = 45.0,
    dump_path: str = "var/hang_dump.txt",
) -> None:
    """Monitor ``task`` and cancel it if no heartbeat is observed."""

    global _last_beat

    Path(dump_path).parent.mkdir(parents=True, exist_ok=True)
    _last_beat = time.monotonic()

    try:
        while not task.done():
            await asyncio.sleep(1.0)
            idle = time.monotonic() - _last_beat
            if idle < idle_seconds:
                continue
            logging.error(
                f"Watchdog: no progress for {float(idle):.1f}s, cancelling task…"
            )
            try:
                with open(dump_path, "w", encoding="utf-8") as fh:
                    fh.write(f"=== HANG DUMP (idle {idle:.1f}s) ===\n")
                    fh.write(f"Task: {task!r}\n")
                    # Dump *all* threads to help investigate deadlocks.
                    faulthandler.dump_traceback(file=fh, all_threads=True)
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.error("Watchdog: dump failed: %r", exc)
            task.cancel("watchdog")
            break
    except asyncio.CancelledError:  # pragma: no cover - defensive cleanup
        pass


async def _jobqueue_watchdog(context: ContextTypes.DEFAULT_TYPE) -> None:
    """PTB JobQueue callback: отслеживает глобальные heartbeat и реагирует на тишину."""

    idle = time.time() - last_touch()  # Вычисляем длительность тишины по глобальному таймштампу
    if idle <= settings.WATCHDOG_TIMEOUT_SEC:  # Если прогресс укладывается в допустимый интервал
        return  # Ничего не делаем, давая задаче продолжить работу
    if settings.WATCHDOG_ENFORCE_CANCEL:  # При включённом жёстком режиме пытаемся остановить приложение
        logging.error(
            "Watchdog: no progress for %.1fs, cancelling task…",
            idle,
        )  # Сообщаем о превышении таймаута перед остановкой
        try:
            await context.application.stop()  # Просим PTB завершить обработку апдейтов
        except Exception:
            pass  # Игнорируем ошибки, чтобы watchdog не падал сам
    else:  # В мягком режиме только предупреждаем и обновляем отметку
        logging.warning(
            "Watchdog: no progress for %.1fs (soft mode, no cancel).",
            idle,
        )  # Фиксируем предупреждение в журнале
        touch("watchdog-soft")  # Обновляем таймштамп, чтобы избежать спама одинаковыми предупреждениями


def install(app: Application) -> None:
    """Подключить PTB watchdog к ``app.job_queue`` с подходящим интервалом проверок."""

    if not settings.WATCHDOG_ENABLED:  # Проверяем, разрешён ли watchdog в конфигурации
        logging.info("Watchdog disabled by config.")  # Сообщаем в лог о намеренном отключении
        return  # Завершаем функцию без добавления задач в JobQueue
    period = max(1.0, min(5.0, settings.WATCHDOG_TIMEOUT_SEC / 10.0))  # Подбираем период проверок
    app.job_queue.run_repeating(
        _jobqueue_watchdog,
        interval=period,
        first=period,
        name="emailbot-watchdog",
    )  # Регистрируем периодическую задачу в JobQueue
    logging.info(
        "Watchdog installed: timeout=%.1fs, period=%.1fs, enforce=%s",
        settings.WATCHDOG_TIMEOUT_SEC,
        period,
        settings.WATCHDOG_ENFORCE_CANCEL,
    )  # Логируем параметры watchdog для диагностики
