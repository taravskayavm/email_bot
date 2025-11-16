"""Проверки нормализации списка исправлений."""

import pytest  # type: ignore  # Импортируем Pytest для написания тестов

from emailbot.bot_handlers import _normalize_repairs  # Импортируем тестируемую функцию


def test_normalize_repairs_keeps_tuples_and_casts_to_str():
    """Убеждаемся, что кортежи сохраняются и приводятся к строкам."""

    raw = [  # Список входных исправлений
        ("old@example.com", "new@example.com"),  # Базовый кортеж для проверки
        ("old@example.com", "new@example.com"),  # Повторный кортеж должен исчезнуть
        ("foo", 123),  # Значение, требующее приведения к строке
    ]

    result = _normalize_repairs(raw)  # Выполняем нормализацию

    assert result.count(("old@example.com", "new@example.com")) == 1  # Проверяем, что дубликат удалён
    assert ("foo", "123") in result  # Убеждаемся, что значения приведены к строкам


def test_normalize_repairs_parses_arrow_string_with_unicode_arrow():
    """Проверяем поддержку строкового формата с юникод-стрелкой."""

    raw = [  # Список входных исправлений
        " bad@example.com  \t→  good@example.com ",  # Строка с лишними пробелами и стрелкой
    ]

    result = _normalize_repairs(raw)  # Выполняем нормализацию

    assert ("bad@example.com", "good@example.com") in result  # Проверяем корректность парсинга


def test_normalize_repairs_parses_arrow_string_with_ascii_arrow():
    """Проверяем поддержку строкового формата с ASCII-стрелкой."""

    raw = [  # Список входных исправлений
        " bad2@example.com  ->  good2@example.com ",  # Строка с ASCII-стрелкой
    ]

    result = _normalize_repairs(raw)  # Выполняем нормализацию

    assert ("bad2@example.com", "good2@example.com") in result  # Проверяем корректность парсинга


def test_normalize_repairs_ignores_invalid_strings():
    """Убеждаемся, что некорректные строки пропускаются."""

    raw = [  # Список входных исправлений
        "no arrow here",  # Строка без стрелки
        "one-side only → ",  # Строка без правой части
        " → missing-left@example.com",  # Строка без левой части
    ]

    result = _normalize_repairs(raw)  # Выполняем нормализацию

    assert result == []  # Ждём пустой результат


def test_normalize_repairs_preserves_first_occurrence_order_and_uniqueness():
    """Проверяем сохранение порядка первого появления элементов."""

    raw = [  # Список входных исправлений
        "a@example.com → b@example.com",  # Первая уникальная пара
        ("c@example.com", "d@example.com"),  # Вторая уникальная пара
        "a@example.com → b@example.com",  # Дубликат первой пары
        ("c@example.com", "d@example.com"),  # Дубликат второй пары
    ]

    result = _normalize_repairs(raw)  # Выполняем нормализацию

    assert result == [  # Сравниваем с ожидаемым порядком
        ("a@example.com", "b@example.com"),  # Ожидаем первую уникальную пару
        ("c@example.com", "d@example.com"),  # Ожидаем вторую уникальную пару
    ]
