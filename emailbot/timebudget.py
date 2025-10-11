"""Simple time budget helper for cooperative extraction workflows."""
from __future__ import annotations

import time
from typing import Optional

__all__ = ["TimeBudget"]


class TimeBudget:
    """Track elapsed time and raise ``TimeoutError`` when exhausted."""

    def __init__(self, seconds: Optional[float] = None) -> None:
        self._deadline: Optional[float]
        if seconds is None or seconds <= 0:
            self._deadline = None
        else:
            self._deadline = time.monotonic() + float(seconds)

    def remaining(self) -> Optional[float]:
        """Return remaining seconds or ``None`` if unlimited."""

        if self._deadline is None:
            return None
        return max(self._deadline - time.monotonic(), 0.0)

    def checkpoint(self) -> None:
        """Raise ``TimeoutError`` if the budget is exhausted."""

        if self._deadline is None:
            return
        if time.monotonic() >= self._deadline:
            raise TimeoutError("time budget exceeded")

    def expired(self) -> bool:
        """Return ``True`` if the budget has been exhausted."""

        if self._deadline is None:
            return False
        return time.monotonic() >= self._deadline
