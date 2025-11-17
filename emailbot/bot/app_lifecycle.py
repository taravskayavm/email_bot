"""Helpers for starting and stopping optional runtime services (watchdog, etc.)."""

from __future__ import annotations  # Поддерживаем отложенные аннотации в типах

import asyncio  # Нужен для фоновых задач
import logging  # Используем для информирования о состоянии watchdog
import time  # Сравниваем времена последнего прогресса

from emailbot import runtime_progress, settings  # Импортируем настройки и маркер прогресса

log = logging.getLogger(__name__)  # Локальный логгер для структурированных сообщений
_wd_task: asyncio.Task | None = None  # Ссылка на запущенный watchdog
_stop_event: asyncio.Event | None = None  # Событие остановки для корректного завершения

async def _watchdog_runner() -> None:
    """Фоновая корутина, отслеживающая отсутствие прогресса и пишущая предупреждения."""

    timeout = float(settings.WATCHDOG_TIMEOUT_SEC)  # Считываем порог ожидания прогресса
    period = max(1.0, min(5.0, timeout / 10.0))  # Определяем частоту проверок в разумных пределах
    log.info("Internal watchdog started: timeout=%.1fs, period=%.1fs", timeout, period)  # Сообщаем о запуске
    while _stop_event and not _stop_event.is_set():  # Работаем, пока не запросили остановку
        last = runtime_progress.last_touch()  # Читаем время последнего прогресса
        gap = time.time() - last  # Вычисляем длительность простоя
        if gap > timeout:  # Если тишина превышает порог
            log.warning(f"Watchdog: no progress for {gap:.1f}s (internal). Waiting…")  # Фиксируем предупреждение
            runtime_progress.touch("watchdog-poke")  # Деликатно помечаем активность
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=period)  # Ждём сигнал или тайм-аут
        except asyncio.TimeoutError:
            pass  # При тайм-ауте просто повторяем проверку
    log.info("Internal watchdog stopped.")  # Сообщаем о штатном завершении

async def on_app_start() -> None:
    """Инициализировать фоновые сервисы при запуске приложения."""

    global _wd_task, _stop_event  # Указываем на изменение глобальных переменных
    if settings.WATCHDOG_ENABLED:  # Запускаем watchdog только при активном флаге
        if _wd_task and not _wd_task.done():  # Избегаем повторного старта
            return  # Сервис уже работает
        _stop_event = asyncio.Event()  # Создаём новое событие остановки
        _wd_task = asyncio.create_task(_watchdog_runner(), name="emailbot-watchdog")  # Запускаем фонового сторожа

async def on_app_stop() -> None:
    """Остановить фоновые сервисы при корректном завершении."""

    global _wd_task, _stop_event  # Работаем с глобальными ссылками
    if _stop_event:  # Если событие существует
        _stop_event.set()  # Сообщаем о необходимости завершиться
    if _wd_task:  # Если задача была создана
        try:
            await asyncio.wait_for(_wd_task, timeout=1.5)  # Ждём аккуратного завершения
        except Exception:
            pass  # Игнорируем сбои остановки, чтобы не мешать выключению
