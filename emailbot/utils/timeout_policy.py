"""Helpers for adaptive timeout calculations."""

from __future__ import annotations

from pathlib import Path

from emailbot import config
from emailbot.utils.logging_setup import get_logger

logger = get_logger(__name__)


def compute_pdf_timeout(pdf_path: Path) -> int:
    """Calculate adaptive PDF extraction timeout."""

    try:
        if not config.PDF_ADAPTIVE_TIMEOUT:
            return max(1, int(config.PDF_EXTRACT_TIMEOUT))
        size_bytes = pdf_path.stat().st_size
        size_mb = max(0.0, float(size_bytes) / (1024.0 * 1024.0))
        timeout = config.PDF_TIMEOUT_BASE + config.PDF_TIMEOUT_PER_MB * size_mb
        if timeout < config.PDF_TIMEOUT_MIN:
            timeout = config.PDF_TIMEOUT_MIN
        if timeout > config.PDF_TIMEOUT_MAX:
            timeout = config.PDF_TIMEOUT_MAX
        return max(1, int(round(timeout)))
    except Exception:
        logger.debug("Falling back to fixed PDF timeout", exc_info=True)
        return max(1, int(config.PDF_EXTRACT_TIMEOUT))
