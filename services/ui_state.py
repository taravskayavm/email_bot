from __future__ import annotations

import json
from typing import Optional

from config import UI_STATE_PATH, ALLOW_FOREIGN_DEFAULT


def _load() -> dict:
    try:
        return json.loads(UI_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: dict) -> None:
    try:
        UI_STATE_PATH.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


def foreign_allowed_for_batch(batch_id: str, chat_id: Optional[int] = None) -> bool:
    data = _load()
    key = f"{chat_id or ''}:{batch_id}"
    value = data.get(key)
    if value is None:
        return ALLOW_FOREIGN_DEFAULT
    return bool(value)


def toggle_foreign_for_batch(batch_id: str, chat_id: Optional[int] = None) -> bool:
    data = _load()
    key = f"{chat_id or ''}:{batch_id}"
    new_val = not bool(data.get(key, ALLOW_FOREIGN_DEFAULT))
    data[key] = new_val
    _save(data)
    return new_val


__all__ = [
    "foreign_allowed_for_batch",
    "toggle_foreign_for_batch",
]
