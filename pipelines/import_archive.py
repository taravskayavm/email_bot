"""Utilities for extracting supported documents from ZIP archives."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable
from zipfile import ZipFile

SUPPORTED_EXTS = {".pdf", ".docx", ".xlsx", ".xls"}  # compare in lower-case


def _iter_supported_files(root: Path) -> Iterable[Path]:
    """Yield files under ``root`` with extensions from :data:`SUPPORTED_EXTS`."""

    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTS:
            yield path


def extract_archive(zip_path: Path, out_dir: Path) -> list[Path]:
    """Extract ``zip_path`` into ``out_dir`` and collect supported files.

    The archive is unpacked preserving the original directory structure. The
    returned list contains every supported document located anywhere inside the
    extracted tree (i.e. the search is recursive). Extension matching is
    case-insensitive so that files such as ``REPORT.PDF`` are detected.
    """

    out_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path) as archive:
        archive.extractall(out_dir)
    return list(_iter_supported_files(out_dir))


__all__ = ["SUPPORTED_EXTS", "extract_archive"]
