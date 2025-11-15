"""Tests for validating configurable send delay parsing."""

# Включаем будущее поведение аннотаций типов для согласованной типизации.
from __future__ import annotations

# Используем importlib, чтобы повторно загружать модуль конфигурации после изменения окружения.
import importlib

# Импортируем модуль конфигурации и будем проверять значение задержки после перезагрузки.
import config as config_module


def test_send_delay_parsed_from_environment(monkeypatch):
    """Ensure EMAILBOT_SEND_DELAY_SEC reflects the environment at import time."""

    # Устанавливаем переменную окружения с тестовым значением задержки отправки.
    monkeypatch.setenv("EMAILBOT_SEND_DELAY_SEC", "2.5")
    # Перезагружаем модуль конфигурации, чтобы он перечитал новое значение из окружения.
    reloaded = importlib.reload(config_module)
    try:
        # Проверяем, что параметр в конфигурации совпадает с ожидаемым значением.
        assert reloaded.EMAILBOT_SEND_DELAY_SEC == 2.5
    finally:
        # Удаляем временно установленную переменную окружения независимо от исхода теста.
        monkeypatch.delenv("EMAILBOT_SEND_DELAY_SEC", raising=False)
        # Перезагружаем конфигурацию повторно, чтобы восстановить исходное состояние для других тестов.
        importlib.reload(config_module)
