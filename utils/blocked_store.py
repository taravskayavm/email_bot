from __future__ import annotations

from pathlib import Path
from typing import Iterable

from filelock import FileLock

from .email_normalize import normalize_email


class BlockedStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.lock = FileLock(str(self.path) + ".lock")

    def read_all(self) -> set[str]:
        if not self.path.exists():
            return set()
        with self.lock:
            with self.path.open("r", encoding="utf-8") as handler:
                return {line.strip().lower() for line in handler if line.strip()}

    def add_many(self, emails: Iterable[str]) -> int:
        existing = self.read_all()
        to_add: set[str] = set()
        for email in emails:
            normalized = normalize_email(email)
            if normalized and normalized not in existing:
                to_add.add(normalized)
        if not to_add:
            return 0
        with self.lock:
            with self.path.open("a", encoding="utf-8") as handler:
                for email in sorted(to_add):
                    handler.write(email + "\n")
        return len(to_add)

    def contains(self, email: str) -> bool:
        normalized = normalize_email(email)
        if not normalized:
            return False
        return normalized in self.read_all()
