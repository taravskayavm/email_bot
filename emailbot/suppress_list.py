"""Helpers for managing the global e-mail block list.

The block list lives under the shared data directory (``EMAILBOT_DATA_DIR`` when
set, otherwise the current working directory).  The helpers exposed here provide
an API for checking and updating that file while keeping basic thread-safety
guarantees.
"""

from __future__ import annotations

import os
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
    _BLOCKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    values = [value.rstrip("\n") for value in items]
    if values:
        data = "\n".join(values) + "\n"
    else:
        data = ""
    _BLOCKLIST_PATH.write_text(data, encoding="utf-8")


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

    del reason  # retained for compatibility with older callers
    with _LOCK:
        return _add_items_locked(emails)


def add_blocked_email(email: str, reason: str | None = None) -> bool:
    """Compatibility wrapper that adds a single email to the block list."""

    return add_to_blocklist(email, reason=reason)


def add_to_blocklist(email: str, reason: str | None = None) -> bool:
    """Add ``email`` to the block list if it is non-empty and absent."""

    del reason  # compatibility placeholder
    normalized = _normalize(email)
    if not normalized:
        return False
    with _LOCK:
        added = _add_items_locked([normalized])
        return added > 0


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
