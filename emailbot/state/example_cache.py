"""Caching helpers for storing example pagination context per chat."""  # Краткое описание назначения модуля

from __future__ import annotations  # Подключаем будущие аннотации для совместимости

import random  # Используем для перемешивания списков примеров
import time  # Используем для отслеживания времени жизни кэша
from dataclasses import dataclass, field  # Используем dataclass для удобного хранения данных
from typing import Dict, List  # Импортируем типы для статической проверки

EXAMPLES_TTL_SECONDS: int = 2 * 60 * 60  # 2 часа времени жизни контекста примеров


@dataclass  # Используем декоратор dataclass для описания пагинатора
class ListPager:
    """Stateful pager that returns chunks of three items with reshuffle on wrap."""  # Описываем назначение пагинатора

    items: List  # Список элементов для пагинации
    idx: int = 0  # Текущая позиция в списке

    def next_chunk(self) -> List:
        """Return the next trio of items, reshuffling when the end is reached."""  # Объясняем работу метода

        if not self.items:  # Проверяем, есть ли вообще элементы
            return []  # Возвращаем пустой список, если элементов нет
        items_count: int = len(self.items)  # Вычисляем количество элементов в списке
        if self.idx >= items_count:  # Проверяем, не достигли ли конца списка
            self.idx = 0  # Сбрасываем индекс на начало
            random.shuffle(self.items)  # Перемешиваем элементы для разнообразия
        end_index: int = min(self.idx + 3, items_count)  # Вычисляем индекс окончания выборки
        chunk = self.items[self.idx:end_index]  # Берём подсписок из трёх элементов
        self.idx = end_index  # Сдвигаем текущий индекс вперёд
        return chunk  # Возвращаем выбранные элементы


@dataclass  # Используем dataclass для хранения полного контекста примеров
class ExamplesContext:
    """Container storing pagination state and metadata for examples."""  # Объясняем назначение структуры

    created_ts: float = field(default_factory=lambda: time.time())  # Фиксируем время создания контекста
    source_kind: str = "parse"  # Источник данных: "parse" или "send"
    source_id: str = ""  # Произвольный идентификатор источника
    cooldown_total: int = 0  # Количество адресов в кулдауне
    foreign_total: int = 0  # Количество иностранных адресов
    cooldown_pager: ListPager = field(default_factory=lambda: ListPager([]))  # Пагинатор для кулдауна
    foreign_pager: ListPager = field(default_factory=lambda: ListPager([]))  # Пагинатор для иностранных адресов

    def expired(self) -> bool:
        """Return True when the context exceeds the allowed time-to-live."""  # Объясняем смысл функции

        return (time.time() - self.created_ts) > EXAMPLES_TTL_SECONDS  # Сравниваем возраст контекста с TTL


_CACHE: Dict[int, ExamplesContext] = {}  # Глобальное хранилище контекстов по идентификатору чата


def put_context(chat_id: int, ctx: ExamplesContext) -> None:
    """Store the provided context for the given chat identifier."""  # Описываем назначение функции

    _CACHE[chat_id] = ctx  # Сохраняем контекст в кэш


def get_context(chat_id: int) -> ExamplesContext | None:
    """Retrieve the context for a chat identifier, removing expired entries."""  # Объясняем назначение функции

    ctx = _CACHE.get(chat_id)  # Извлекаем контекст из кэша
    if ctx and ctx.expired():  # Проверяем, существует ли контекст и не устарел ли он
        _CACHE.pop(chat_id, None)  # Удаляем просроченный контекст
        return None  # Возвращаем None для обозначения отсутствия данных
    return ctx  # Возвращаем актуальный контекст
