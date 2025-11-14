"""Smoke tests for the geo-domains heuristics."""
# Описание модуля.

# Импортируем функцию классификации доменов по географии.
from utils.geo_domains import is_foreign_email  # Используем целевую функцию в проверках


def test_com_is_local() -> None:  # Определяем проверку для домена .com
    """Ensure ``.com`` domains stay in the local bucket by default."""
    # Поясняем цель проверки.

    # Вызываем целевую функцию для адреса с доменом .com.
    result = is_foreign_email("user@x.com")  # Получаем результат классификации
    # Убеждаемся, что адрес признан локальным.
    assert result is False  # Проверяем, что адрес остаётся локальным


def test_ion_ru_is_foreign() -> None:  # Определяем проверку для домена ion.ru
    """Confirm that ``ion.ru`` is treated as foreign according to the config."""
    # Поясняем цель проверки.

    # Вызываем функцию для адреса с доменом из списка иностранных доменов.
    result = is_foreign_email("user@ion.ru")  # Получаем результат классификации
    # Проверяем, что адрес классифицирован как иностранный.
    assert result is True  # Проверяем, что адрес отмечен как иностранный
