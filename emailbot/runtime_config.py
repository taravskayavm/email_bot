import importlib
import json
from pathlib import Path
from typing import Any, Dict, Optional

_PATH = Path("var/runtime_config.json")
_CACHE: Dict[str, Any] | None = None


def _ensure_dir():
    _PATH.parent.mkdir(parents=True, exist_ok=True)


def _load() -> Dict[str, Any]:
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    try:
        data = json.loads(_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _CACHE = data
            return data
    except Exception:
        pass
    _CACHE = {}
    return _CACHE


def get(key: str, default: Any = None) -> Any:
    return _load().get(key, default)


def get_many(keys: list[str]) -> Dict[str, Any]:
    data = _load()
    return {k: data.get(k) for k in keys}


def _reload_config_module() -> None:
    try:
        from emailbot import config as config_module
    except Exception:
        return
    try:
        importlib.reload(config_module)
    except Exception:
        pass


def set(key: str, value: Any) -> None:
    data = _load()
    data[key] = value
    _ensure_dir()
    _PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _reload_config_module()


def set_many(pairs: Dict[str, Any]) -> None:
    data = _load()
    data.update(pairs)
    _ensure_dir()
    _PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _reload_config_module()


def clear(keys: Optional[list[str]] = None) -> None:
    global _CACHE
    if keys is None:
        _CACHE = None
        if _PATH.exists():
            _PATH.unlink(missing_ok=True)
        _reload_config_module()
        return

    data = _load()
    removed = False
    for k in keys:
        if k in data:
            data.pop(k, None)
            removed = True
    if removed:
        _ensure_dir()
        _PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _reload_config_module()
