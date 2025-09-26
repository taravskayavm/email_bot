import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from utils.paths import ensure_parent, expand_path

_STATE_ENV = os.getenv("MASS_STATE_PATH", "var/mass_state.json")
STATE_PATH: Path = expand_path(_STATE_ENV)

# In-memory representation of the persisted state.  The file stores a mapping
# of ``chat_id`` (as string) to an arbitrary dictionary.  This allows multiple
# chats to run mass-mailing sessions independently.
_state_cache: Dict[str, Dict[str, Any]] | None = None


def _load_all() -> Dict[str, Dict[str, Any]]:
    """Load the full state mapping from disk (lazy)."""

    global _state_cache
    if _state_cache is not None:
        return _state_cache
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                _state_cache = data  # type: ignore[assignment]
            else:  # pragma: no cover - corrupt state
                _state_cache = {}
    except FileNotFoundError:
        _state_cache = {}
    except Exception:  # pragma: no cover - rare
        _state_cache = {}
    return _state_cache


def _save_all(state: Dict[str, Dict[str, Any]]) -> None:
    global _state_cache

    data: Dict[str, Dict[str, Any]]
    if isinstance(state, dict) and all(isinstance(v, dict) for v in state.values()):
        _state_cache = state
        data = state
    else:
        data = _state_cache or {}

    ensure_parent(STATE_PATH)
    tmp = STATE_PATH.with_name(f"{STATE_PATH.name}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def load_chat_state(chat_id: int) -> Optional[Dict[str, Any]]:
    """Return saved state for ``chat_id`` if present."""

    data = _load_all()
    return data.get(str(chat_id))


def save_chat_state(chat_id: int, state: Dict[str, Any]) -> None:
    """Persist ``state`` for ``chat_id`` to disk."""

    data = _load_all()
    data[str(chat_id)] = state
    _save_all(data)


def clear_chat_state(chat_id: int) -> None:
    """Remove saved state for ``chat_id`` from disk."""

    data = _load_all()
    if str(chat_id) in data:
        del data[str(chat_id)]
        _save_all(data)


def get_batch(chat_id: int) -> Optional[str]:
    """Return the current batch identifier for ``chat_id`` if any."""

    state = load_chat_state(chat_id) or {}
    batch = state.get("batch_id")
    return batch if isinstance(batch, str) else None


def set_batch(chat_id: int, batch_id: str) -> None:
    """Persist ``batch_id`` for ``chat_id`` without altering other fields."""

    state = load_chat_state(chat_id) or {}
    state["batch_id"] = batch_id
    save_chat_state(chat_id, state)


def clear_batch(chat_id: int) -> None:
    """Remove only the batch identifier for ``chat_id``."""

    state = load_chat_state(chat_id) or {}
    if "batch_id" in state:
        del state["batch_id"]
        save_chat_state(chat_id, state)


# Backwards compatibility helpers -------------------------------------------------

def load_state() -> Optional[Dict[str, Any]]:  # pragma: no cover - legacy
    return _load_all()


def save_state(state: Dict[str, Any]) -> None:  # pragma: no cover - legacy
    _save_all(state)  # type: ignore[arg-type]


def clear_state() -> None:  # pragma: no cover - legacy
    try:
        os.remove(STATE_PATH)
    except FileNotFoundError:
        pass
