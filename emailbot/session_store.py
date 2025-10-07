from __future__ import annotations

import json
import os
from typing import Any

from .settings import LAST_SUMMARY_DIR

os.makedirs(LAST_SUMMARY_DIR, exist_ok=True)


def _path(chat_id: int) -> str:
    return os.path.join(LAST_SUMMARY_DIR, f"summary_{chat_id}.json")


def save_last_summary(chat_id: int, data: dict[str, Any]) -> None:
    with open(_path(chat_id), "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)


def load_last_summary(chat_id: int) -> dict[str, Any] | None:
    path = _path(chat_id)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None
