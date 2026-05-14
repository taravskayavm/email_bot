"""Helpers for light-weight normalization before extracting e-mail addresses."""

from __future__ import annotations  # Обеспечиваем совместимость с аннотациями нового образца

import re  # Используем регулярные выражения для очистки и деобфускации

# Предкомпилируем шаблон для мягких пробелов и спецпробелов, встречающихся в PDF
_RE_SOFT_SPACES = re.compile(r"[ \t\u00A0\u2007\u202F]+")  # Объединяем разные пробелы в единый
# Детектируем конструкции вида "name [at] domain" и подобные вариации
_RE_BROKEN_AT = re.compile(
    r"(\b[a-zA-Z0-9._%+\-]+)\s*(?:\[\s*at\s*\]|\(at\)|\{at\}|@\s*)\s*([a-zA-Z0-9.\-]+)",
    re.IGNORECASE,
)  # Задаём паттерн для восстановления символа '@'
# Распознаём «dot» или типографские точки между частями адреса
_RE_BROKEN_DOT = re.compile(
    r"([a-zA-Z0-9._%+\-@])\s*(?:\[\s*dot\s*\]|\(dot\)|\{dot\}|[·•])\s*([a-zA-Z0-9.\-])",
    re.IGNORECASE,
)  # Описываем варианты точек для восстановления
# Удаляем мягкие переносы слов, чтобы не ломать адреса при склейке
_RE_SOFT_HYPHEN = re.compile(r"\u00AD")  # Находим символ мягкого дефиса


def normalize_for_email(text: str) -> str:
    """Return a gently normalised string optimised for subsequent e-mail parsing."""

    if not text:  # Проверяем, что вход не пустой, чтобы избежать лишних операций
        return ""  # Для пустого ввода сразу возвращаем пустую строку
    cleaned = text  # Создаём рабочую копию исходного текста
    cleaned = _RE_SOFT_HYPHEN.sub("", cleaned)  # Удаляем мягкие переносы, мешающие адресам
    cleaned = _RE_SOFT_SPACES.sub(" ", cleaned)  # Нормализуем экзотические пробелы до обычного пробела
    cleaned = _RE_BROKEN_DOT.sub(r"\1.\2", cleaned)  # Восстанавливаем точки в обфусцированных адресах
    cleaned = _RE_BROKEN_AT.sub(r"\1@\2", cleaned)  # Подставляем символ '@' в типичных маскировках
    return cleaned  # Возвращаем нормализованный текст для дальнейшего анализа
