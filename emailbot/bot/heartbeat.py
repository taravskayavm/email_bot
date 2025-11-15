"""Heartbeat helper to keep Telegram conversations alive during long tasks."""

from __future__ import annotations  # Поддерживаем отложенные аннотации типов

import asyncio  # Работаем с асинхронными задачами
import time  # Фиксируем время последних пингов
from aiogram import Bot  # Используем Telegram Bot API
from emailbot import runtime_progress  # Сообщаем глобальному watchdog о прогрессе


class Heartbeat:
    """Асинхронный «пульс» для длительных операций, поддерживающий chat action."""

    def __init__(
        self,
        bot: Bot,
        chat_id: int,
        *,
        interval_sec: float,
        force_after_sec: float,
    ) -> None:
        """Настраиваем heartbeat: бот, чат и интервалы отправки сигналов."""

        self.bot = bot  # Сохраняем объект бота для отправки действий
        self.chat_id = chat_id  # Запоминаем идентификатор чата
        self.interval = max(1.0, float(interval_sec))  # Гарантируем минимальный интервал в секунду
        self.force_after = max(self.interval, float(force_after_sec))  # Не даём форс-таймауту быть меньше интервала
        self._last_touch = time.time()  # Фиксируем момент последней активности
        self._stop = asyncio.Event()  # Создаём событие для остановки фоновой задачи
        self._task: asyncio.Task | None = None  # Храним ссылку на запущенную корутину

    def touch(self) -> None:
        """Сигнализируем о прогрессе, чтобы отложить форс-пинг."""

        self._last_touch = time.time()  # Обновляем момент активности при явном прогрессе

    async def _run(self) -> None:
        """Фоновый цикл, который периодически шлёт chat action."""

        while not self._stop.is_set():  # Пока не поступил запрос на остановку
            now = time.time()  # Считываем текущее время
            try:
                await self.bot.send_chat_action(chat_id=self.chat_id, action="typing")  # Отправляем признак «печатаю»
            except Exception:
                pass  # Игнорируем любые сетевые ошибки, чтобы не прерывать цикл
            try:
                runtime_progress.touch("heartbeat")  # Отмечаем прогресс для внутреннего watchdog
            except Exception:
                pass  # Не позволяем вспомогательным ошибкам нарушать heartbeat
            if (now - self._last_touch) > self.force_after:  # Проверяем, не наступил ли таймаут молчания
                self._last_touch = now  # Сбрасываем таймер активности после форс-пинга
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)  # Ждём либо остановку, либо таймаут
            except asyncio.TimeoutError:
                pass  # Тихо продолжаем цикл, если время ожидания истекло

    async def start(self) -> None:
        """Запускаем heartbeat, если он ещё не работает."""

        if self._task and not self._task.done():  # Проверяем, не бежит ли задача уже
            return  # Повторно не стартуем, чтобы не дублировать пинги
        self._stop.clear()  # Сбрасываем событие остановки перед запуском
        self._task = asyncio.create_task(self._run(), name="emailbot-heartbeat")  # Создаём фоновую задачу

    async def stop(self) -> None:
        """Останавливаем heartbeat и ждём завершения фоновой задачи."""

        self._stop.set()  # Сигнализируем о необходимости остановиться
        if self._task:  # Проверяем, была ли задача создана
            try:
                await asyncio.wait_for(self._task, timeout=1.5)  # Даём задаче завершиться с тайм-аутом
            except Exception:
                pass  # На ошибки завершения просто закрываем глаза
