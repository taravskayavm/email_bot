"""Utilities for tracking global runtime progress timestamps."""

import threading  # Потокобезопасность при одновременных обновлениях
import time  # Получаем текущие временные метки
import logging  # Фиксируем heartbeat в логах

_lock = threading.Lock()  # Глобальный мьютекс для безопасного доступа к таймштампу
_last_touch = time.time()  # Последнее время зафиксированного прогресса
_logger = logging.getLogger("root")  # Используем корневой логгер для заметности сообщений

def touch(reason: str = "heartbeat") -> None:
    """Обновить глобальный таймштамп прогресса и записать мягкий heartbeat в лог."""

    global _last_touch  # Указываем на использование внешней переменной
    with _lock:  # Гарантируем атомарность обновления таймштампа
        _last_touch = time.time()  # Запоминаем текущее время как момент прогресса
    try:
        _logger.info("Progress %s at %.3f", reason, _last_touch)  # Пишем отметку в лог
    except Exception:  # Не позволяем логированию прерывать основной поток
        pass

def last_touch() -> float:
    """Вернуть последний зарегистрированный момент прогресса."""

    with _lock:  # Читаем таймштамп под блокировкой для согласованности
        return _last_touch  # Возвращаем время последнего heartbeat
