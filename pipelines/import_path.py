"""Collect supported files from a downloads directory."""

from __future__ import annotations

from pathlib import Path

SUPPORTED_EXTS = {".pdf", ".docx", ".xlsx", ".xls"}


def collect_files(root: Path) -> list[Path]:
    """Return supported files found under ``root`` recursively."""

    return [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTS
    ]


__all__ = ["SUPPORTED_EXTS", "collect_files"]
