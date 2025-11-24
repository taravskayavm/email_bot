"""Utility helpers for tracking long-running per-chat asyncio tasks."""

from __future__ import annotations  # Позволяет использовать подсказки типов, объявленные позже.

import asyncio  # Используется для определения типа asyncio.Task.
from typing import Dict, Optional  # Предоставляет типы словаря и опционального значения.

_tasks_by_chat: Dict[int, asyncio.Task] = {}  # Глобальное хранилище задач, привязанных к chat_id.


def register_long_task(chat_id: int, task: asyncio.Task) -> None:  # Сохраняет задачу, чтобы можно было управлять ею позже.
    """Сохраняет долгую задачу для указанного chat_id, чтобы можно было отменить её позже."""
    _tasks_by_chat[chat_id] = task  # Связываем chat_id с задачей для дальнейшего управления.


def get_long_task(chat_id: int) -> Optional[asyncio.Task]:  # Возвращает задачу для заданного chat_id, если она была сохранена.
    """Возвращает зарегистрированную задачу для chat_id или None, если запись отсутствует."""
    return _tasks_by_chat.get(chat_id)  # Получаем задачу из словаря, если такая запись есть.


def cancel_long_task(chat_id: int) -> bool:  # Отменяет задачу и сообщает, была ли она найдена.
    """Отменяет и удаляет задачу для chat_id; возвращает True, если задача существовала."""
    task = _tasks_by_chat.get(chat_id)  # Пытаемся найти задачу для переданного chat_id.
    if task is None:  # Проверяем, найдена ли задача.
        return False  # Нечего отменять, сообщаем об отсутствии задачи.

    if not task.done():  # Если задача ещё выполняется.
        task.cancel()  # Помечаем задачу на отмену для корректного завершения.

    _tasks_by_chat.pop(chat_id, None)  # Удаляем запись из словаря, чтобы не хранить устаревшие ссылки.
    return True  # Сообщаем вызывающему коду, что задача была найдена (и при необходимости отменена).
