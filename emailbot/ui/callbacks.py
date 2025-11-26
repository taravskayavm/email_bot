"""Utility constants describing callback identifiers for example pagination UI."""  # Предоставляем краткое описание назначения файла

from __future__ import annotations  # Обеспечиваем совместимость аннотаций с будущими версиями Python

# Callback identifiers for example pagination interactions
CB_EXAMPLES_INIT: str = "ex:init"  # Callback для открытия блока примеров
CB_EXAMPLES_BACK: str = "ex:back"  # Callback для возвращения к отчёту
CB_EXAMPLES_MORE_COOLDOWN: str = "ex:more:cd"  # Callback для загрузки ещё трёх адресов из кулдауна
CB_EXAMPLES_MORE_FOREIGN: str = "ex:more:fr"  # Callback для загрузки ещё трёх иностранных адресов

# Backwards-compatible short aliases if потребуются альтернативные имена
CB_EX_INIT: str = CB_EXAMPLES_INIT  # Сохраняем краткое имя для совместимости
CB_EX_BACK: str = CB_EXAMPLES_BACK  # Сохраняем краткое имя для совместимости
CB_EX_MORE_CD: str = CB_EXAMPLES_MORE_COOLDOWN  # Сохраняем краткое имя для совместимости
CB_EX_MORE_FR: str = CB_EXAMPLES_MORE_FOREIGN  # Сохраняем краткое имя для совместимости

# Context keys describing the origin of examples
CTX_SOURCE_KIND: str = "source_kind"  # Ключ контекста: источник данных ("parse" или "send")
CTX_SOURCE_ID: str = "source_id"  # Ключ контекста: идентификатор источника данных
