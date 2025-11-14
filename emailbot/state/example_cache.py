"""In-memory cache for storing paginated examples per chat."""

from __future__ import annotations  # Разрешаем поздние аннотации для совместимости

import random  # Используем для случайного перемешивания списков после полного обхода
import time  # Импортируем время для реализации TTL
from dataclasses import dataclass, field  # Используем dataclass для компактных контейнеров
from typing import Dict, List, Tuple, TypeVar, Generic  # Импортируем типы для строгой типизации коллекций

# Обозначаем тип элементов списка для пагинатора
T = TypeVar("T")  # Универсальный параметр типа для списка примеров

# Определяем время жизни кэша в секундах (2 часа)
TTL_SECONDS: int = 2 * 60 * 60  # Гарантируем, что контекст не будет использоваться слишком долго


@dataclass
class ListPager(Generic[T]):  # Создаём универсальный пагинатор для списков
    items: List[T]  # Храним полный список элементов для выдачи по частям
    idx: int = 0  # Отслеживаем текущую позицию в списке для цикличного прохода

    def next_chunk(self, size: int = 3) -> List[T]:  # Возвращаем следующую порцию элементов
        """Return ``size`` items from ``items`` with reshuffle after each cycle."""

        if not self.items:  # Проверяем, есть ли вообще элементы для вывода
            return []  # Возвращаем пустой список, если элементы отсутствуют
        total: int = len(self.items)  # Фиксируем количество элементов для удобства
        if self.idx >= total:  # Если достигли конца списка, начинаем заново
            self.idx = 0  # Сбрасываем индекс на начало
            random.shuffle(self.items)  # Перемешиваем элементы, чтобы примеры не повторялись подряд
        end: int = min(self.idx + size, total)  # Вычисляем правую границу среза
        chunk: List[T] = self.items[self.idx:end]  # Получаем нужный фрагмент списка
        self.idx = end  # Сохраняем новую позицию для следующего вызова
        return chunk  # Возвращаем выбранные элементы


@dataclass
class ExamplesContext:  # Сохраняем информацию о примерах для конкретного чата
    created_ts: float = field(default_factory=time.time)  # Фиксируем момент создания контекста
    source_kind: str = "parse"  # Указываем тип источника («parse» или «send»)
    source_id: str = ""  # Храним идентификатор отчёта, чтобы можно было восстановить контекст
    cooldown_total: int = 0  # Общее количество адресов с ограничением 180 дней
    foreign_total: int = 0  # Общее количество адресов с иностранными доменами
    cooldown_pager: ListPager[Tuple[str, str]] = field(  # Пагинатор для пар (email, дата)
        default_factory=lambda: ListPager([])
    )
    foreign_pager: ListPager[str] = field(  # Пагинатор для списка иностранных адресов
        default_factory=lambda: ListPager([])
    )

    def is_expired(self) -> bool:  # Проверяем, истёк ли срок действия контекста
        """Return ``True`` if the context lifetime exceeded ``TTL_SECONDS``."""

        return (time.time() - self.created_ts) > TTL_SECONDS  # Сравниваем прошедшее время с лимитом


_CACHE: Dict[int, ExamplesContext] = {}  # Храним контексты по идентификатору чата


def put_context(chat_id: int, ctx: ExamplesContext) -> None:  # Сохраняем контекст для чата
    """Store ``ctx`` for ``chat_id`` replacing previous value."""

    _CACHE[chat_id] = ctx  # Запоминаем контекст в словаре


def get_context(chat_id: int) -> ExamplesContext | None:  # Получаем контекст, если он ещё валиден
    """Return stored context if present and not expired."""

    ctx = _CACHE.get(chat_id)  # Пытаемся извлечь контекст из кэша
    if ctx and ctx.is_expired():  # Проверяем наличие и актуальность контекста
        _CACHE.pop(chat_id, None)  # Удаляем просроченный контекст, чтобы не расходовать память
        return None  # Возвращаем None, поскольку данные устарели
    return ctx  # Возвращаем найденный контекст или None, если его не было


__all__ = [  # Экспортируем публичные элементы модуля
    "TTL_SECONDS",  # Делимся значением TTL для внешнего использования
    "ListPager",  # Предоставляем класс пагинатора
    "ExamplesContext",  # Предоставляем контейнер контекста примеров
    "put_context",  # Экспортируем функцию записи контекста
    "get_context",  # Экспортируем функцию чтения контекста
]
