from __future__ import annotations

import logging
import os
import re
import shutil
import threading
from pathlib import Path
from typing import Iterable, Set

from utils.paths import expand_path

_DEFAULT_BLOCKLIST = Path("~/.emailbot/blocked_emails.txt")
_LEGACY_BLOCKLIST = Path("var/blocked_emails.txt")

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from .extraction_common import normalize_email as _normalize_email
except Exception:  # pragma: no cover - fallback when extraction module unavailable
    def _normalize_email(value: str) -> str:
        return (value or "").strip().lower()


_LOCK = threading.RLock()
# Path can be overridden via environment variable BLOCKED_EMAILS_FILE (preferred)
# or legacy BLOCKED_LIST_PATH / BLOCKED_EMAILS_PATH; default is ~/.emailbot/blocked_emails.txt.
_ENV_BLOCKLIST = (
    os.getenv("BLOCKED_EMAILS_FILE")
    or os.getenv("BLOCKED_LIST_PATH")
    or os.getenv("BLOCKED_EMAILS_PATH")
)
if _ENV_BLOCKLIST:
    _BLOCKED_PATH: Path = expand_path(_ENV_BLOCKLIST)
    _ALLOW_LEGACY_FALLBACK = False
else:
    _BLOCKED_PATH = expand_path(_DEFAULT_BLOCKLIST)
    _ALLOW_LEGACY_FALLBACK = True
_LEGACY_BLOCKED_PATH = expand_path(_LEGACY_BLOCKLIST)
_CACHE: Set[str] = set()
_MTIME: float | None = None
_SOURCE_PATH: Path | None = None

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


def _select_blocklist_source() -> Path | None:
    """Return path to read the block-list from, migrating legacy file if needed."""

    try:
        _BLOCKED_PATH.stat()
        return _BLOCKED_PATH
    except FileNotFoundError:
        pass

    if not _ALLOW_LEGACY_FALLBACK:
        return None

    legacy = _LEGACY_BLOCKED_PATH
    if legacy == _BLOCKED_PATH:
        return None

    try:
        legacy.stat()
    except FileNotFoundError:
        return None

    try:
        _ensure_dir(_BLOCKED_PATH)
        shutil.copy2(legacy, _BLOCKED_PATH)
        return _BLOCKED_PATH
    except Exception as exc:
        logger.warning(
            "Cannot migrate legacy block-list from %s to %s: %s",
            legacy,
            _BLOCKED_PATH,
            exc,
        )
        return legacy


def _load_file() -> None:
    global _CACHE, _MTIME, _SOURCE_PATH

    path = _select_blocklist_source()
    if path is None:
        _CACHE = set()
        _MTIME = None
        _SOURCE_PATH = None
        return

    try:
        stat = path.stat()
    except FileNotFoundError:
        _CACHE = set()
        _MTIME = None
        _SOURCE_PATH = None
        return

    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        _CACHE = set()
        _MTIME = stat.st_mtime
        _SOURCE_PATH = path
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
    _SOURCE_PATH = path


def _ensure_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def load_blocked_set() -> Set[str]:
    """Return the stop-list as a set, reading from disk when necessary."""

    with _LOCK:
        _load_file()
        return set(_CACHE)


def _atomic_write(lines: Iterable[str]) -> None:
    """Persist ``lines`` to the block-list file atomically."""

    _ensure_dir(_BLOCKED_PATH)
    tmp = _BLOCKED_PATH.with_name(_BLOCKED_PATH.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as handler:
        for line in lines:
            handler.write(line.rstrip("\n") + "\n")
    os.replace(tmp, _BLOCKED_PATH)


def save_blocked_set(items: Iterable[str]) -> None:
    """Persist the provided items as the canonical block-list."""

    normalized = {
        _normalize(item)
        for item in items
        if item is not None
    }
    normalized.discard("")
    ordered = sorted(normalized)

    with _LOCK:
        global _CACHE, _MTIME
        _atomic_write(ordered)
        _CACHE = set(ordered)
        try:
            _MTIME = _BLOCKED_PATH.stat().st_mtime
        except Exception:
            _MTIME = None


def add_blocked(emails: Iterable[str], reason: str | None = None) -> int:
    """Add e-mail addresses to the block-list and persist the change."""

    del reason  # Reason is currently informational only.
    candidates = {
        _normalize(email)
        for email in (emails or [])
        if email is not None
    }
    candidates.discard("")
    if not candidates:
        return 0

    with _LOCK:
        global _CACHE, _MTIME
        refresh_if_changed()
        before = set(_CACHE)
        updated = before | candidates
        if updated == _CACHE:
            return 0
        _atomic_write(sorted(updated))
        _CACHE = updated
        try:
            _MTIME = _BLOCKED_PATH.stat().st_mtime
        except Exception:
            _MTIME = None
        return len(updated) - len(before)


def refresh_if_changed() -> None:
    """Reload the stop-list if the backing file has changed."""

    with _LOCK:
        candidate: Path | None = None
        mtime: float | None = None

        try:
            stat = _BLOCKED_PATH.stat()
        except FileNotFoundError:
            if _ALLOW_LEGACY_FALLBACK:
                try:
                    stat = _LEGACY_BLOCKED_PATH.stat()
                    candidate = _LEGACY_BLOCKED_PATH
                    mtime = stat.st_mtime
                except FileNotFoundError:
                    candidate = None
                    mtime = None
            else:
                candidate = None
                mtime = None
        else:
            candidate = _BLOCKED_PATH
            mtime = stat.st_mtime

        if candidate != _SOURCE_PATH or mtime != _MTIME:
            _load_file()


def init_blocked(path: str | os.PathLike[str] | None = None) -> None:
    """Initialise the stop-list cache explicitly (e.g. during startup)."""

    global _BLOCKED_PATH, _ALLOW_LEGACY_FALLBACK, _SOURCE_PATH

    with _LOCK:
        if path is not None:
            _BLOCKED_PATH = expand_path(path)
            _ALLOW_LEGACY_FALLBACK = False
        elif _ENV_BLOCKLIST:
            _BLOCKED_PATH = expand_path(_ENV_BLOCKLIST)
            _ALLOW_LEGACY_FALLBACK = False
        else:
            _BLOCKED_PATH = expand_path(_DEFAULT_BLOCKLIST)
            _ALLOW_LEGACY_FALLBACK = True
        try:
            _ensure_dir(_BLOCKED_PATH)
            _BLOCKED_PATH.touch(exist_ok=True)
        except Exception as exc:
            logger.warning("Cannot initialise block-list file %s: %s", _BLOCKED_PATH, exc)
        _SOURCE_PATH = None
        _load_file()


def add_to_blocklist(email: str) -> bool:
    """Add a normalised e-mail to the block-list file.

    Returns ``True`` if the address is successfully persisted or already present,
    ``False`` if the operation failed.
    """

    norm = _normalize(email)
    if not norm:
        return False

    added = add_blocked([norm])
    if added:
        return True

    with _LOCK:
        refresh_if_changed()
        return norm in _CACHE


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
    "load_blocked_set",
    "save_blocked_set",
    "add_blocked",
    "add_to_blocklist",
]
