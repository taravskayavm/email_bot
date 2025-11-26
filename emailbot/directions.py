"""Утилиты для работы со справочником направлений рассылки."""

from __future__ import annotations  # Обеспечиваем совместимость аннотаций с будущими версиями Python.

from dataclasses import dataclass  # Подключаем декоратор dataclass для описания структур данных.
from functools import lru_cache  # Импортируем кеширующий декоратор для оптимизации загрузки данных.
from pathlib import Path  # Используем Path для работы с путями в файловой системе.
from typing import Dict  # Подключаем словари для аннотаций возвращаемых значений.

import json  # Библиотека json нужна для чтения справочника направлений.


@dataclass(frozen=True)  # Фиксируем неизменяемость структуры данных после создания.
class DirectionInfo:
    """Описание направления рассылки."""

    code: str  # Строковый код направления, используемый как ключ.
    title: str  # Человеко-читаемое название направления.
    signature: str | None = None  # Необязательная подпись, если требуется особое оформление.


def _default_labels_path() -> Path:
    """
    Путь к файлу templates/_labels.json относительно корня проекта.

    Предполагается стандартная структура:
      <project_root>/templates/_labels.json
    """

    # Определяем корневую директорию проекта, поднимаясь на уровень выше текущего файла.
    project_root = Path(__file__).resolve().parent.parent
    # Формируем путь к файлу со справочником направлений.
    return project_root / "templates" / "_labels.json"


@lru_cache(maxsize=1)  # Кешируем результат, чтобы не перечитывать файл при повторных вызовах.
def load_directions_labels(path: str | Path | None = None) -> Dict[str, DirectionInfo]:
    """
    Загрузить справочник направлений из templates/_labels.json.

    Формат файла (пример):
    {
      "highmedicine": { "title": "Медицина ВО", "signature": "old" },
      "sport": { "title": "Физкультура и спорт", "signature": "old" }
    }
    """

    # Если путь передан извне, используем его, иначе применяем путь по умолчанию.
    labels_path = Path(path) if path is not None else _default_labels_path()
    # Оборачиваем чтение файла в try, чтобы корректно обработать отсутствие справочника.
    try:
        # Открываем файл и загружаем JSON-данные.
        with labels_path.open("r", encoding="utf-8") as file_obj:
            raw = json.load(file_obj)
    except FileNotFoundError:
        # Если файл не найден, возвращаем пустой справочник.
        return {}

    # Проверяем, что корневой объект JSON является словарём.
    if not isinstance(raw, dict):
        return {}

    # Создаём результирующий словарь с типизированными объектами DirectionInfo.
    result: Dict[str, DirectionInfo] = {}
    # Перебираем пары «код направления» — «метаданные».
    for code, meta in raw.items():
        # Пропускаем записи с некорректным типом ключа.
        if not isinstance(code, str):
            continue
        # Игнорируем записи, если значение не представлено словарём с данными.
        if not isinstance(meta, dict):
            continue
        # Извлекаем и нормализуем название направления.
        title = str(meta.get("title") or "").strip()
        # Пропускаем запись, если название отсутствует.
        if not title:
            continue
        # Извлекаем необязательное поле подписи.
        signature = meta.get("signature")
        # Создаём объект DirectionInfo и помещаем его в итоговый словарь.
        result[code] = DirectionInfo(
            code=code,
            title=title,
            signature=str(signature) if signature is not None else None,
        )
    # Возвращаем собранный справочник направлений.
    return result


def resolve_direction_title(code: str) -> str | None:
    """
    Вернуть человеко-читаемое название направления по его коду.

    Если код отсутствует в labels-файле — вернуть None.
    """

    # Нормализуем входной код, удаляя пустые значения и лишние пробелы.
    normalized_code = (code or "").strip()
    # Прекращаем работу, если код пустой после нормализации.
    if not normalized_code:
        return None
    # Загружаем справочник направлений (используя кеш, чтобы не читать файл повторно).
    labels = load_directions_labels()
    # Получаем описание направления из словаря.
    info = labels.get(normalized_code)
    # Возвращаем название, если описание найдено, иначе None.
    return info.title if info is not None else None
