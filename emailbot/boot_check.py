from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

REQUIRED_DIRS = [
    "var",
    "var/ocr_cache",
]

REQUIRED_ENV = [
    "EMAIL_ADDRESS",
    "EMAIL_PASSWORD",
]


def _ensure_dirs(base: Path) -> None:
    for rel in REQUIRED_DIRS:
        p = base / rel
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log.error("Cannot create required dir: %s (%r)", p, e)
            raise


def _check_env() -> list[str]:
    missing = []
    for k in REQUIRED_ENV:
        if not os.getenv(k):
            missing.append(k)
    return missing


def _ocr_available_if_needed() -> bool:
    # OCR is optional; we only warn if auto-ocr is requested but engine unavailable.
    try:
        from emailbot.config import PDF_OCR_AUTO, TESSERACT_CMD
    except Exception:
        return True
    if not PDF_OCR_AUTO:
        return True
    try:
        import shutil
        import pytesseract  # noqa: F401

        if TESSERACT_CMD:
            return os.path.exists(TESSERACT_CMD)
        return shutil.which("tesseract") is not None
    except Exception:
        return False


def run_boot_check(project_root: Path) -> None:
    _ensure_dirs(project_root)
    missing = _check_env()
    if missing:
        # Fail fast with clear message. No noisy dump of config.
        raise SystemExit(
            f"[BOOT] Missing required env vars: {', '.join(missing)}. "
            "Set them in .env before starting the bot."
        )
    if not _ocr_available_if_needed():
        log.warning(
            "[BOOT] PDF_OCR_AUTO is enabled but Tesseract is not available. OCR will be skipped."
        )
