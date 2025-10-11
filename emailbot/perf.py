"""Lightweight performance logging utilities."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, Optional


PERF_LOG_PATH = os.getenv("PERF_LOG_PATH", "var/perf.log")


def perf_log(event: str, elapsed_ms: float, extra: Optional[Dict[str, Any]] = None) -> None:
    """Append a structured performance log entry."""

    directory = os.path.dirname(PERF_LOG_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    payload: Dict[str, Any] = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "event": event,
        "elapsed_ms": round(elapsed_ms, 3),
    }
    if extra:
        payload.update(extra)
    with open(PERF_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


class PerfTimer:
    """Context manager that measures wall time and logs it via :func:`perf_log`."""

    def __init__(self, event: str, extra: Optional[Dict[str, Any]] = None):
        self.event = event
        self.extra = extra or {}
        self._start: float | None = None

    def __enter__(self) -> "PerfTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        assert self._start is not None, "PerfTimer used without entering context"
        elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        perf_log(self.event, elapsed_ms, self.extra)
        # Propagate exception, if any.
        return False
