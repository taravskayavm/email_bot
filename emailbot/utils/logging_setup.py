"""Utilities for consistent logger configuration used across modules."""

from __future__ import annotations

import logging
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def get_logger(name: str, *, level: Optional[int] = None) -> logging.Logger:
    """Return a logger with a default stream handler if none configured."""

    logger = logging.getLogger(name)
    if logger.handlers:
        if level is not None:
            logger.setLevel(level)
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(level if level is not None else logging.INFO)
    logger.propagate = False
    return logger


__all__ = ["get_logger"]
