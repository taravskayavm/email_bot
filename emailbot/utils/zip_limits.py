"""Utilities for quick ZIP archive validation before heavy parsing."""

from __future__ import annotations

import os
import zipfile
from typing import Tuple

__all__ = ["validate_zip_safely", "MAX_FILES_PER_ZIP", "MAX_TOTAL_UNCOMPRESSED_BYTES"]


try:
    MAX_FILES_PER_ZIP
except NameError:  # pragma: no cover - module attributes may be pre-set
    MAX_FILES_PER_ZIP = 500
try:
    MAX_TOTAL_UNCOMPRESSED_BYTES
except NameError:  # pragma: no cover
    MAX_TOTAL_UNCOMPRESSED_BYTES = 200 * 1024 * 1024  # 200 MB


def _iter_zip_infos(zf: zipfile.ZipFile):
    """Yield non-directory members from ``zf``."""

    for info in zf.infolist():
        if info.is_dir():
            continue
        yield info


def _estimate_depth(name: str) -> int:
    """Roughly estimate archive nesting depth based on separators."""

    return name.count("/") + name.count("\\")


def validate_zip_safely(
    zip_path: str,
    max_files: int = 1000,
    max_total_uncompressed_mb: int = 500,
    max_depth: int = 3,
) -> Tuple[bool, str | None]:
    """Lightweight validation of a ZIP archive before deep parsing.

    The function checks the following constraints:
    * the number of files does not exceed ``max_files``;
    * the total uncompressed size does not exceed ``max_total_uncompressed_mb``;
    * nesting depth is limited by ``max_depth``;
    * detects potential zip-bombs via compression ratio heuristics.

    Returns a tuple ``(ok, reason)`` where ``reason`` is provided only when
    validation fails.
    """

    if not os.path.exists(zip_path):
        return False, "файл архива не найден"

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            total_uncompressed = 0
            files_cnt = 0
            for info in _iter_zip_infos(zf):
                files_cnt += 1
                if files_cnt > max_files:
                    return False, f"слишком много файлов (> {max_files})"

                file_size = getattr(info, "file_size", 0) or 0
                compress_size = getattr(info, "compress_size", 0) or 0
                if file_size > 0 and compress_size > 0:
                    ratio = file_size / max(1, compress_size)
                    if ratio > 5000 and file_size > 50 * 1024 * 1024:
                        return False, "подозрение на zip-бомбу (аномально высокая компрессия)"

                total_uncompressed += file_size
                if (total_uncompressed / (1024.0 * 1024.0)) > max_total_uncompressed_mb:
                    return False, f"слишком большой распакованный объём (> {max_total_uncompressed_mb} МБ)"

                depth = _estimate_depth(info.filename)
                if depth > max_depth:
                    return False, f"слишком глубокая вложенность (> {max_depth})"
    except zipfile.BadZipFile:
        return False, "файл повреждён или не является ZIP"
    except Exception as exc:
        return False, f"ошибка чтения ZIP: {exc}"

    return True, None
