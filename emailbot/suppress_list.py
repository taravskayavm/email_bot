"""Helpers for managing the global e-mail block list.

The block list lives under the shared data directory (``EMAILBOT_DATA_DIR`` when
set, otherwise the current working directory).  The helpers exposed here provide
an API for checking and updating that file while keeping basic thread-safety
guarantees.
"""

from __future__ import annotations

import os
import re  # Используем регулярные выражения для валидации адресов e-mail
import tempfile  # Применяем временные файлы для атомарной записи блок-листа
from pathlib import Path
from threading import RLock
from typing import Iterable, Set

__all__ = [
    "BLOCKED_EMAILS_PATH",
    "blocklist_path",
    "is_blocked",
    "add_to_blocklist",
    "add_blocked",
    "add_blocked_email",
    "load_blocked_set",
    "get_blocked_set",
    "save_blocked_set",
    "get_blocked_count",
    "refresh_if_changed",
    "invalidate_cache",
    "init_blocked",
    "_set_blocked_path_for_tests",
]


_LOCK = RLock()


def _resolve_data_dir() -> Path:
    """Determine the directory where persistent data should be stored."""

    override = os.getenv("EMAILBOT_DATA_DIR")
    if override:
        try:
            base = Path(override).expanduser()
        except Exception:
            base = Path(override)
    else:
        base = Path.cwd()
    return base.resolve()


def _default_blocklist_path() -> Path:
    return _resolve_data_dir() / "blocked_emails.txt"

_DEFAULT_BLOCKLIST_PATH = _default_blocklist_path()  # Сохраняем путь по умолчанию для возможного отката
_BLOCKLIST_PATH = _DEFAULT_BLOCKLIST_PATH  # Используем путь по умолчанию как актуальный путь к файлу
_BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)

BLOCKED_EMAILS_PATH: Path = _BLOCKLIST_PATH

_CACHE: Set[str] = set()
_MTIME: float | None = None
EMAIL_VALIDATION_RE = re.compile(  # Регулярное выражение для базовой проверки адреса без пробелов
    r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
)  # Разрешаем любые символы кроме пробелов и повторных @, поддерживая Unicode


def blocklist_path() -> Path:
    """Return the path of the shared block list file."""

    return _BLOCKLIST_PATH


def _normalize(email: str) -> str:
    return (email or "").strip().lower()


def _read_blocklist_locked() -> Set[str]:
    if not _BLOCKLIST_PATH.exists():
        return set()
    text = _BLOCKLIST_PATH.read_text(encoding="utf-8")
    return {
        line.strip().lower()
        for line in text.splitlines()
        if line.strip()
    }


def _write_blocklist_locked(items: Iterable[str]) -> None:
    _BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)  # Убеждаемся, что каталог существует
    fd, tmp_path = tempfile.mkstemp(  # Создаём временный файл для атомарной записи списка
        prefix="blocked_",
        suffix=".tmp",
        dir=str(_BLOCKLIST_PATH.parent),
    )
    os.close(fd)  # Закрываем файловый дескриптор, чтобы писать в текстовом режиме
    try:
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as handle:  # Открываем временный файл для записи
            for value in items:  # Перебираем нормализованные адреса
                handle.write(value.rstrip("\n") + "\n")  # Записываем каждый адрес отдельной строкой
        os.replace(tmp_path, _BLOCKLIST_PATH)  # Атомарно заменяем основной файл временным
    finally:
        try:
            os.remove(tmp_path)  # Удаляем временный файл, если он ещё существует
        except FileNotFoundError:
            pass  # Игнорируем отсутствие файла — это штатная ситуация после os.replace


def _ensure_loaded_locked() -> None:
    global _CACHE, _MTIME

    try:
        mtime = _BLOCKLIST_PATH.stat().st_mtime
    except FileNotFoundError:
        mtime = None
    if mtime == _MTIME:
        return

    _CACHE = _read_blocklist_locked()
    _MTIME = mtime


def refresh_if_changed() -> None:
    """Reload the cached block list if the underlying file changed."""

    with _LOCK:
        _ensure_loaded_locked()


def load_blocked_set() -> Set[str]:
    """Return a copy of the cached block list, refreshing if needed."""

    with _LOCK:
        _ensure_loaded_locked()
        return set(_CACHE)


def get_blocked_set() -> Set[str]:
    return load_blocked_set()


def get_blocked_count() -> int:
    with _LOCK:
        _ensure_loaded_locked()
        return len(_CACHE)


def _add_items_locked(items: Iterable[str]) -> int:
    global _CACHE, _MTIME

    cleaned = {
        _normalize(item)
        for item in items
        if item is not None
    }
    cleaned.discard("")
    if not cleaned:
        return 0

    _ensure_loaded_locked()
    before = set(_CACHE)
    updated = before | cleaned
    if updated == _CACHE:
        return 0

    ordered = sorted(updated)
    _write_blocklist_locked(ordered)
    _CACHE = set(ordered)
    try:
        _MTIME = _BLOCKLIST_PATH.stat().st_mtime
    except FileNotFoundError:
        _MTIME = None
    return len(updated) - len(before)


def add_blocked(emails: Iterable[str], reason: str | None = None) -> int:
    """Add multiple emails to the block list."""

    del reason  # Параметр поддерживается для совместимости со старыми вызовами
    added_total = 0  # Счётчик успешно добавленных адресов
    for email in emails:  # Перебираем переданную коллекцию адресов
        success, status = add_blocked_email(email)  # Переиспользуем основную функцию добавления
        if success and status == "added":  # Засчитываем только реальные добавления
            added_total += 1  # Увеличиваем счётчик на единицу
    return added_total  # Возвращаем количество новых адресов


def add_blocked_email(email: str, reason: str | None = None) -> tuple[bool, str]:
    """Добавить e-mail в блок-лист с пояснением результата."""

    del reason  # Параметр сохранён для обратной совместимости, но не используется
    normalized = _normalize(email)  # Нормализуем адрес: тримминг и перевод в нижний регистр
    if not normalized or not EMAIL_VALIDATION_RE.match(normalized):  # Проверяем базовую валидность адреса
        return False, "invalid"  # Сообщаем вызывающему коду о некорректном формате
    with _LOCK:  # Гарантируем потокобезопасность при чтении и записи файла
        global _CACHE, _MTIME  # Указываем на изменение глобальных структур кэша
        _ensure_loaded_locked()  # Загружаем актуальное состояние блок-листа при необходимости
        if normalized in _CACHE:  # Проверяем, добавлялся ли адрес ранее
            return False, "exists"  # Возвращаем информацию о наличии адреса без записи
        updated = sorted(_CACHE | {normalized})  # Формируем отсортированное множество с новым адресом
        _write_blocklist_locked(updated)  # Перезаписываем файл блок-листа через временный файл
        _CACHE = set(updated)  # Обновляем кэш актуальным набором адресов
        try:
            _MTIME = _BLOCKLIST_PATH.stat().st_mtime  # Фиксируем новое время модификации файла
        except FileNotFoundError:
            _MTIME = None  # Если файл внезапно исчез, сбрасываем отметку времени
    return True, "added"  # Сообщаем об успешном добавлении адреса


def add_to_blocklist(email: str, reason: str | None = None) -> bool:
    """Add ``email`` to the block list if it is non-empty and absent."""

    result, _ = add_blocked_email(email, reason=reason)  # Переиспользуем новую функцию и игнорируем текстовый статус
    return result  # Возвращаем только булево значение для старых вызовов


def is_blocked(email: str) -> bool:
    normalized = _normalize(email)
    if not normalized:
        return False
    with _LOCK:
        _ensure_loaded_locked()
        return normalized in _CACHE


def save_blocked_set(items: Iterable[str]) -> None:
    with _LOCK:
        normalized_values = {
            _normalize(item)
            for item in items
            if item is not None
        }
        normalized_values.discard("")
        normalized = sorted(normalized_values)
        _write_blocklist_locked(normalized)
        global _CACHE, _MTIME
        _CACHE = set(normalized)
        try:
            _MTIME = _BLOCKLIST_PATH.stat().st_mtime
        except FileNotFoundError:
            _MTIME = None


def invalidate_cache() -> None:
    global _MTIME
    with _LOCK:
        _MTIME = None


def init_blocked(path: str | Path | None = None) -> None:
    """Initialise the block list file (optionally overriding the path)."""

    global _BLOCKLIST_PATH, BLOCKED_EMAILS_PATH, _CACHE, _MTIME

    with _LOCK:
        if path is not None:
            _BLOCKLIST_PATH = Path(path)
            BLOCKED_EMAILS_PATH = _BLOCKLIST_PATH
            _BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        _BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not _BLOCKLIST_PATH.exists():
            _BLOCKLIST_PATH.touch()
        _CACHE = set()
        try:
            _CACHE = _read_blocklist_locked()
            _MTIME = _BLOCKLIST_PATH.stat().st_mtime
        except FileNotFoundError:
            _MTIME = None


def _set_blocked_path_for_tests(path: str | Path | None) -> None:
    """Переключить файл блок-листа на альтернативный путь для юнит-тестов."""

    target_path = _DEFAULT_BLOCKLIST_PATH if path is None else Path(path)  # Выбираем путь для инициализации
    init_blocked(target_path)  # Переинициализируем блок-лист на выбранном пути
