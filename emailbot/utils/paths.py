from __future__ import annotations

from pathlib import Path
import os


_ROOT = Path(__file__).resolve().parents[2]


def project_root() -> Path:
    return _ROOT


def resolve_project_path(p: str | os.PathLike[str]) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    return _ROOT / path
