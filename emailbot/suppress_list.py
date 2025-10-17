from __future__ import annotations

import logging
import os
import re
import threading
from pathlib import Path
from typing import Set

from utils.paths import expand_path

_DEFAULT_BLOCKLIST = os.path.join("~", ".emailbot", "blocked_emails.txt")

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from .extraction_common import normalize_email as _normalize_email
except Exception:  # pragma: no cover - fallback when extraction module unavailable
    def _normalize_email(value: str) -> str:
        return (value or "").strip().lower()


_LOCK = threading.RLock()
# Path can be overridden via environment variable BLOCKED_LIST_PATH (preferred)
# or legacy BLOCKED_EMAILS_PATH; default is ~/.emailbot/blocked_emails.txt.
_ENV_BLOCKLIST = os.getenv("BLOCKED_LIST_PATH") or os.getenv("BLOCKED_EMAILS_PATH")
_BLOCKED_PATH: Path = expand_path(_ENV_BLOCKLIST or _DEFAULT_BLOCKLIST)
_CACHE: Set[str] = set()
_MTIME: float | None = None

_LEADING_DOTS_RE = re.compile(r"^\.+")
_LEADING_DIGITS_RE = re.compile(r"^\d{1,2}(?=[A-Za-z])")


def _normalize(email: str) -> str:
    """Return canonical representation for stop-list comparison."""

    try:
        value = _normalize_email(email)
    except Exception:
        value = (email or "").strip().lower()
    value = _LEADING_DOTS_RE.sub("", value)
    value = _LEADING_DIGITS_RE.sub("", value)
    return value


def _load_file() -> None:
    global _CACHE, _MTIME

    path = _BLOCKED_PATH
    if not path.exists():
        _CACHE = set()
        _MTIME = None
        return

    try:
        stat = path.stat()
    except FileNotFoundError:
        _CACHE = set()
        _MTIME = None
        return

    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        _CACHE = set()
        _MTIME = stat.st_mtime
        return

    items: Set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        norm = _normalize(stripped)
        if norm:
            items.add(norm)
    _CACHE = items
    _MTIME = stat.st_mtime


def refresh_if_changed() -> None:
    """Reload the stop-list if the backing file has changed."""

    with _LOCK:
        try:
            stat = _BLOCKED_PATH.stat()
            mtime = stat.st_mtime
        except FileNotFoundError:
            mtime = None
        if mtime != _MTIME:
            _load_file()


def init_blocked(path: str | os.PathLike[str] | None = None) -> None:
    """Initialise the stop-list cache explicitly (e.g. during startup)."""

    global _BLOCKED_PATH

    with _LOCK:
        if path is not None:
            _BLOCKED_PATH = expand_path(path)
        try:
            _BLOCKED_PATH.parent.mkdir(parents=True, exist_ok=True)
            _BLOCKED_PATH.touch(exist_ok=True)
        except Exception as exc:
            logger.warning("Cannot initialise block-list file %s: %s", _BLOCKED_PATH, exc)
        _load_file()


def add_to_blocklist(email: str) -> bool:
    """Add a normalised e-mail to the block-list file.

    Returns ``True`` if the address is successfully persisted or already present,
    ``False`` if the operation failed.
    """

    norm = _normalize(email)
    if not norm:
        return False

    with _LOCK:
        global _MTIME
        refresh_if_changed()
        if norm in _CACHE:
            return True

        try:
            _BLOCKED_PATH.parent.mkdir(parents=True, exist_ok=True)
            with _BLOCKED_PATH.open("a", encoding="utf-8") as handler:
                handler.write(norm + "\n")
            _CACHE.add(norm)
            try:
                _MTIME = _BLOCKED_PATH.stat().st_mtime
            except Exception:
                pass
            return True
        except Exception as exc:
            logger.warning("Failed to persist %s to block-list %s: %s", norm, _BLOCKED_PATH, exc)
            return False


def is_blocked(email: str) -> bool:
    """Return True if ``email`` is present in ``blocked_emails.txt``."""

    refresh_if_changed()
    return _normalize(email) in _CACHE


def get_blocked_count() -> int:
    """Return the number of cached blocked addresses."""

    refresh_if_changed()
    return len(_CACHE)


def get_blocked_set() -> Set[str]:
    """Return a snapshot of the cached blocked addresses."""

    refresh_if_changed()
    return set(_CACHE)


def invalidate_cache() -> None:
    """Force cache reload on the next access (useful after manual updates)."""

    global _MTIME
    with _LOCK:
        _MTIME = None


__all__ = [
    "init_blocked",
    "refresh_if_changed",
    "is_blocked",
    "get_blocked_count",
    "get_blocked_set",
    "invalidate_cache",
    "add_to_blocklist",
]
