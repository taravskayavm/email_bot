from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from utils.paths import ensure_parent, expand_path


DEFAULTS = {
    "STRICT_OBFUSCATION": True,
    "FOOTNOTE_RADIUS_PAGES": 1,
    "PDF_LAYOUT_AWARE": False,
    "ENABLE_OCR": False,
    "MAX_ASSETS": 8,
    "MAX_SITEMAP_URLS": 200,
    "MAX_DOCS": 30,
    "PER_REQUEST_TIMEOUT": 15,
    "DAILY_SEND_LIMIT": 300,
    "EXTERNAL_SOURCES": {},
}

_SETTINGS_ENV = os.getenv("SETTINGS_PATH", "var/settings.json")
SETTINGS_PATH: Path = expand_path(_SETTINGS_ENV)
_cache: dict[str, Any] | None = None
_mtime: float = 0.0


def _load() -> dict[str, Any]:
    global _cache, _mtime
    try:
        stat = SETTINGS_PATH.stat()
        if _cache is None or stat.st_mtime != _mtime:
            _cache = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            _mtime = stat.st_mtime
    except Exception:
        _cache = {}
        _mtime = 0.0
    return _cache


def _ensure_defaults() -> dict[str, Any]:
    data = _load()
    changed = False
    for k, v in DEFAULTS.items():
        if k not in data:
            data[k] = v
            changed = True
    if changed:
        try:
            ensure_parent(SETTINGS_PATH)
            SETTINGS_PATH.write_text(json.dumps(data), encoding="utf-8")
            stat = SETTINGS_PATH.stat()
            global _mtime, _cache
            _mtime = stat.st_mtime
            _cache = data
        except Exception:
            pass
    return data


def get(name: str, default: Any | None = None) -> Any:
    """Return a setting value ensuring defaults are persisted."""

    data = _ensure_defaults()
    return data.get(name, default)


def set(name: str, value: Any) -> None:
    """Persist a setting value to :data:`SETTINGS_PATH` with validation."""

    allowed = {
        "STRICT_OBFUSCATION": {True, False},
        "FOOTNOTE_RADIUS_PAGES": {0, 1, 2},
        "PDF_LAYOUT_AWARE": {True, False},
        "ENABLE_OCR": {True, False},
    }
    if name in allowed and value not in allowed[name]:
        raise ValueError("invalid value")
    data = _load()
    data[name] = value
    try:
        ensure_parent(SETTINGS_PATH)
        SETTINGS_PATH.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


__all__ = ["get", "set", "DEFAULTS"]
