"""Central logging configuration helpers."""

from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_CONFIGURED = False


def _ensure_parent(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def setup_logging() -> None:
    """Configure the root logger once with console and file handlers."""

    global _CONFIGURED
    if _CONFIGURED:
        return

    base_dir = Path(os.getenv("EMAILBOT_LOG_DIR", "logs")).resolve()
    info_log = base_dir / "emailbot_info.log"
    err_log = base_dir / "emailbot_errors.log"
    _ensure_parent(info_log)
    _ensure_parent(err_log)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(_DEFAULT_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    info_handler = TimedRotatingFileHandler(
        info_log.as_posix(), when="midnight", backupCount=7, encoding="utf-8"
    )
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(fmt)
    root.addHandler(info_handler)

    err_handler = TimedRotatingFileHandler(
        err_log.as_posix(), when="midnight", backupCount=14, encoding="utf-8"
    )
    err_handler.setLevel(logging.ERROR)
    err_handler.setFormatter(fmt)
    root.addHandler(err_handler)

    _CONFIGURED = True


def get_logger(name: str, *, level: Optional[int] = None) -> logging.Logger:
    """Return a configured logger, initialising handlers on first use."""

    setup_logging()
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    return logger


__all__ = ["get_logger", "setup_logging"]
