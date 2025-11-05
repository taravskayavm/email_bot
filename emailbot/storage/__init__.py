"""Storage backends used by the bot."""

from __future__ import annotations

import os
from typing import Iterable, Tuple

_BACKEND = os.getenv("EMAILBOT_STORAGE", "file").lower()

if _BACKEND == "sqlite":
    from .sqlite_store import audit_add as storage_audit_add
    from .sqlite_store import add_blocked as storage_add_blocked
    from .sqlite_store import init as init_storage
    from .sqlite_store import is_blocked as storage_is_blocked
    from .sqlite_store import list_blocked as storage_list_blocked
    try:  # pragma: no cover - defensive initialisation
        init_storage()
    except Exception:
        pass
else:  # pragma: no cover - default lightweight deployments
    def init_storage() -> None:
        return None

    def storage_add_blocked(email: str) -> None:
        return None

    def storage_is_blocked(email: str) -> bool:
        return False

    def storage_list_blocked(limit: int = 100, offset: int = 0) -> Iterable[Tuple[str, str]]:
        del limit, offset
        return []

    def storage_audit_add(email: str, status: str, override: bool = False) -> None:
        del email, status, override
        return None

__all__ = [
    "init_storage",
    "storage_add_blocked",
    "storage_is_blocked",
    "storage_list_blocked",
    "storage_audit_add",
]
