"""Быстрый смоук-тест эвристики определения «иностранных» доменов."""  # Документируем назначение файла

from utils.geo_domains import is_foreign_email  # Импортируем проверяемую функцию


def test_com_is_local() -> None:
    """Адреса в зоне .com считаем локальными по бизнес-правилу."""  # Объясняем суть теста

    assert is_foreign_email("user@x.com") is False  # Проверяем, что .com трактуется как локальный домен


def test_ion_ru_is_foreign(monkeypatch) -> None:
    """Домены ion.ru считаем иностранными, если .ru исключён из локальных TLD."""  # Объясняем условие теста

    monkeypatch.setattr("config.LOCAL_TLDS", [".рф", ".su"], raising=False)  # Исключаем .ru из списка локальных зон
    monkeypatch.setattr("config.LOCAL_DOMAINS_EXTRA", set(), raising=False)  # Сбрасываем явный allow-list доменов
    monkeypatch.setattr("utils.geo_domains.LOCAL_TLDS", [".рф", ".su"], raising=False)  # Обновляем локальный кэш TLD в модуле
    monkeypatch.setattr("utils.geo_domains.LOCAL_DOMAINS_EXTRA", set(), raising=False)  # Обновляем локальный кэш доменов
    assert is_foreign_email("user@ion.ru") is True  # Проверяем, что ion.ru становится иностранным
