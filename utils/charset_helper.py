"""Utilities for decoding byte responses with a best-effort strategy."""

from __future__ import annotations

try:  # pragma: no cover - optional dependency
    from charset_normalizer import from_bytes as _cn_from_bytes
except Exception:  # pragma: no cover - fallback when package is unavailable
    _cn_from_bytes = None


def best_effort_decode(data: bytes) -> str:
    """Decode ``data`` using charset-normalizer when available."""

    if not data:
        return ""
    if _cn_from_bytes is not None:
        try:
            matches = _cn_from_bytes(data)
            best = matches.best() if matches else None
            if best:
                return str(best)
        except Exception:
            pass
    for encoding in ("utf-8", "utf-16", "cp1251", "latin-1"):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("utf-8", "ignore")

