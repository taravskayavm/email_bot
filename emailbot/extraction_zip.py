from __future__ import annotations

import logging
import os
import tempfile
import zipfile
from pathlib import PurePosixPath
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _safe_unlink(path: str, attempts: int = 6, delay: float = 0.2) -> bool:
    import os, time, logging

    log = logging.getLogger(__name__)
    for i in range(attempts):
        try:
            os.remove(path)
            return True
        except PermissionError:
            time.sleep(delay * (i + 1))
        except FileNotFoundError:
            return True
        except Exception:
            log.exception("unlink failed for %s", path)
            break
    return False

ALLOWED_EXTS = {".pdf", ".docx", ".xlsx", ".csv", ".txt", ".html", ".htm", ".zip"}
DENY_EXTS = {
    ".exe",
    ".dll",
    ".js",
    ".bat",
    ".cmd",
    ".sh",
    ".com",
    ".pif",
    ".scr",
    ".cpl",
    ".msi",
    ".msp",
    ".jar",
    ".vbs",
    ".ps1",
    ".php",
}

MAX_FILES = 1000
MAX_SIZE = 100 * 1024 * 1024  # 100 MB
MAX_DEPTH = 3


def _safe_path(name: str) -> bool:
    p = PurePosixPath(name.replace("\\", "/"))
    return not (p.is_absolute() or ".." in p.parts)


def extract_emails_from_zip(
    path: str, stop_event: Optional[object] = None, *, _depth: int = 0
) -> tuple[list["EmailHit"], Dict]:
    from .extraction import (
        EmailHit,
        extract_any_stream,
        merge_footnote_prefix_variants,
        repair_footnote_singletons,
        _dedupe,
    )

    if _depth > MAX_DEPTH:
        logger.warning("zip depth exceeded for %s", path)
        return [], {"errors": ["max depth exceeded"]}

    try:
        z = zipfile.ZipFile(path)
    except Exception:
        return [], {"errors": ["cannot open"]}

    infos = z.infolist()
    if len(infos) > MAX_FILES:
        logger.warning("zip has too many files: %s", path)
        z.close()
        return [], {"errors": ["too many files"]}

    total_size = sum(i.file_size for i in infos)
    if total_size > MAX_SIZE:
        logger.warning("zip too large: %s (%d bytes)", path, total_size)
        z.close()
        return [], {"errors": ["too big"]}

    hits: List[EmailHit] = []
    stats: Dict[str, int] = {}

    for info in infos:
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            break
        name = info.filename
        if info.flag_bits & 0x1:
            logger.warning("encrypted file skipped in zip %s: %s", path, name)
            continue
        if not _safe_path(name):
            logger.warning("unsafe path in zip %s: %s", path, name)
            return [], {"errors": ["unsafe path"]}
        ext = os.path.splitext(name)[1].lower()
        if ext in DENY_EXTS:
            logger.warning("deny-listed extension in zip %s: %s", path, name)
            return [], {"errors": ["forbidden extension"]}
        if ext == ".zip":
            if _depth + 1 > MAX_DEPTH:
                logger.warning("nested zip depth exceeded in %s: %s", path, name)
                return [], {"errors": ["max depth exceeded"]}
            data = z.read(info)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                tmp.write(data)
                tmp_path = tmp.name
            try:
                inner_hits, inner_stats = extract_emails_from_zip(
                    tmp_path, stop_event, _depth=_depth + 1
                )
                for h in inner_hits:
                    suffix = ""
                    if "#" in h.source_ref:
                        suffix = "#" + h.source_ref.split("#", 1)[1]
                    new_ref = f"zip:{path}|{name}{suffix}"
                    hits.append(
                        EmailHit(
                            email=h.email,
                            source_ref=new_ref,
                            origin=h.origin,
                            pre=h.pre,
                            post=h.post,
                        )
                    )
                for k, v in inner_stats.items():
                    if isinstance(v, int):
                        stats[k] = stats.get(k, 0) + v
            finally:
                if not _safe_unlink(tmp_path):
                    logger.warning("temp file still locked, skip delete: %s", tmp_path)
            continue
        if ext not in ALLOWED_EXTS:
            continue
        data = z.read(info)
        inner_hits, inner_stats = extract_any_stream(
            data,
            ext,
            source_ref=f"zip:{path}|{name}",
            stop_event=stop_event,
        )
        hits.extend(inner_hits)
        key = ext.lstrip(".")
        stats[key] = stats.get(key, 0) + 1
        for k, v in inner_stats.items():
            if isinstance(v, int):
                stats[k] = stats.get(k, 0) + v

    z.close()
    hits = merge_footnote_prefix_variants(hits, stats)
    hits = _dedupe(hits)
    hits = repair_footnote_singletons(hits, stats)
    return hits, stats


__all__ = ["extract_emails_from_zip"]
