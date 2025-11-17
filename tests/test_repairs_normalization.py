"""Тесты нормализации списка исправлений."""

from emailbot.bot_handlers import _normalize_repairs  # Импортируем целевую функцию, чтобы проверять её поведение


def test_normalize_repairs_keeps_tuples_and_casts_to_str():
    """Проверяем, что кортежи сохраняются и элементы приводятся к строкам."""

    raw = [  # Формируем исходный набор данных с повторами
        ("old@example.com", "new@example.com"),  # Первая валидная пара
        ("old@example.com", "new@example.com"),  # Дубликат предыдущей пары
        ("foo", 123),  # Второй элемент не строка и должен быть приведён
    ]
    result = _normalize_repairs(raw)  # Запускаем нормализацию
    assert result.count(("old@example.com", "new@example.com")) == 1  # Дубликаты должны исчезнуть
    assert ("foo", "123") in result  # Числовое значение должно стать строкой


def test_normalize_repairs_parses_arrow_string_with_unicode_arrow():
    """Убеждаемся, что записи с юникод-стрелкой разбираются корректно."""

    raw = [  # Формируем строку с юникод-стрелкой
        " bad@example.com  \t→  good@example.com ",  # Набор лишних пробелов имитирует реальные данные
    ]
    result = _normalize_repairs(raw)  # Получаем нормализованный список
    assert ("bad@example.com", "good@example.com") in result  # Проверяем, что пара распознана


def test_normalize_repairs_parses_arrow_string_with_ascii_arrow():
    """Проверяем поддержку ASCII-стрелки."""

    raw = [  # Создаём данные с ASCII-стрелкой
        " bad2@example.com  ->  good2@example.com ",  # Добавляем лишние пробелы для устойчивости
    ]
    result = _normalize_repairs(raw)  # Нормализуем данные
    assert ("bad2@example.com", "good2@example.com") in result  # Проверяем успешный разбор


def test_normalize_repairs_ignores_invalid_strings():
    """Удостоверяемся, что некорректные строки не попадают в результат."""

    raw = [  # Перечисляем проблемные записи
        "no arrow here",  # Строка без стрелки
        "one-side only → ",  # Отсутствует правая часть
        " → missing-left@example.com",  # Отсутствует левая часть
    ]
    result = _normalize_repairs(raw)  # Применяем нормализацию
    assert result == []  # Список должен оказаться пустым


def test_normalize_repairs_preserves_first_occurrence_order_and_uniqueness():
    """Проверяем сохранение порядка первого появления элементов."""

    raw = [  # Собираем повторяющиеся записи
        "a@example.com → b@example.com",  # Строковая запись
        ("c@example.com", "d@example.com"),  # Кортежная запись
        "a@example.com → b@example.com",  # Повтор строки
        ("c@example.com", "d@example.com"),  # Повтор кортежа
    ]
    result = _normalize_repairs(raw)  # Нормализуем
    assert result == [  # Ожидаем сохранить только первые упоминания
        ("a@example.com", "b@example.com"),  # Итоговая первая пара
        ("c@example.com", "d@example.com"),  # Итоговая вторая пара
    ]
