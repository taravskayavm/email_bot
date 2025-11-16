"""PTB JobQueue-powered heartbeat helper for long-running operations."""

from __future__ import annotations  # Включаем отложенные аннотации для совместимости с Python 3.8+

from contextlib import contextmanager  # Создаём настраиваемые контексты для JobQueue heartbeat
from telegram.constants import ChatAction  # Определяем типы chat action, доступные Telegram
from telegram.ext import Application  # Используем PTB Application для доступа к JobQueue

from emailbot import settings  # Читаем интервалы heartbeat из пользовательских настроек
from emailbot.progress_watchdog import touch  # Передаём глобальному watchdog сигнал о прогрессе


@contextmanager
def jobqueue_heartbeat(
    app: Application | None,
    chat_id: int,
    *,
    action: str = ChatAction.TYPING,
):
    """На время работы запускает повторяющийся chat action и обновляет глобальный watchdog."""

    if app is None:  # Если PTB Application недоступно (например, в тестах)
        yield  # Ничего не делаем, но поддерживаем унифицированный интерфейс контекст-менеджера
        return  # Завершаем контекст без запуска JobQueue задачи

    def _beat(context) -> None:
        """Выполняется JobQueue: отправляет chat action и обновляет отметку прогресса."""

        try:
            context.bot.send_chat_action(chat_id=chat_id, action=action)  # Сообщаем пользователю, что бот «пишет»
        except Exception:
            pass  # Сетевые сбои не должны ронять watchdog
        touch("heartbeat")  # Сообщаем глобальному watchdog о текущем прогрессе

    job = app.job_queue.run_repeating(  # Создаём периодическую задачу в PTB JobQueue
        _beat,
        interval=max(1.0, float(settings.HEARTBEAT_SEC)),
        first=0.0,
        name=f"hb-{chat_id}",
    )
    try:
        yield  # Передаём управление вызывающему коду, пока задача поддерживает heartbeat
    finally:
        try:
            job.schedule_removal()  # Удаляем задачу из JobQueue при завершении работы
        except Exception:
            pass  # Сбои при остановке heartbeat считаем некритичными
