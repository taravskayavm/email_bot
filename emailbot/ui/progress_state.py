"""State container for reporting parsing progress to the UI."""

from __future__ import annotations

import time
from threading import Lock
from typing import Optional

from emailbot.config import (
    PROGRESS_UPDATE_EVERY_PAGES,
    PROGRESS_UPDATE_MIN_SEC,
)


class ParseProgress:
    """Thread-safe progress tracker used by long-running parsers."""

    def __init__(
        self,
        phase: str = "init",
        *,
        update_every_pages: Optional[int] = None,
        min_interval_sec: Optional[float] = None,
    ) -> None:
        self._lock = Lock()
        self.phase = phase
        self.pages_scanned = 0
        self.pages_total = 0
        self.found_count = 0
        self.ocr_status = "не требуется"
        self.started_at = time.time()
        self.last_render_at = 0.0
        self._last_summary = ""
        self._last_report_pages = 0
        self._dirty = True
        self._force_render = True
        self._update_every_pages = max(
            1, int(update_every_pages or PROGRESS_UPDATE_EVERY_PAGES)
        )
        self._min_interval = max(
            0.0, float(min_interval_sec or PROGRESS_UPDATE_MIN_SEC)
        )

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------
    def _mark_dirty(self, *, force: bool = False) -> None:
        self._dirty = True
        if force:
            self._force_render = True

    def set_phase(self, phase: str) -> None:
        with self._lock:
            self.phase = phase
            self._mark_dirty(force=True)

    def set_total(self, total: int) -> None:
        with self._lock:
            self.pages_total = max(0, int(total))
            self._mark_dirty(force=True)

    def inc_pages(self, n: int = 1) -> None:
        if n <= 0:
            return
        with self._lock:
            self.pages_scanned += int(n)
            self._mark_dirty()

    def set_found(self, n: int) -> None:
        with self._lock:
            self.found_count = max(0, int(n))
            self._mark_dirty()

    def set_ocr(self, on: bool | str) -> None:
        if isinstance(on, str):
            status = on.strip() or "не требуется"
        else:
            status = "вкл" if on else "не требуется"
        with self._lock:
            self.ocr_status = status
            self._mark_dirty(force=True)

    def set_ocr_status(self, status: str) -> None:
        self.set_ocr(status)

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------
    def _format_summary_locked(self, now: float) -> str:
        elapsed = int(now - self.started_at)
        mm, ss = divmod(max(0, elapsed), 60)
        t_str = f"{mm:02d}:{ss:02d}"
        total = self.pages_total or "?"
        ocr = self.ocr_status or "не требуется"
        phase = (self.phase or "парсинг").strip()
        return (
            f"🔎 {phase} · {self.pages_scanned}/{total} стр. · "
            f"найдено {self.found_count} · OCR: {ocr} · {t_str}"
        )

    def maybe_render_summary(self, force: bool = False) -> Optional[str]:
        """Return a formatted summary when throttling conditions permit."""

        with self._lock:
            if not self._dirty and not force:
                return None
            now = time.time()
            pages_delta = self.pages_scanned - self._last_report_pages
            time_delta = now - self.last_render_at
            if not (force or self._force_render):
                if pages_delta < self._update_every_pages:
                    return None
                if time_delta < self._min_interval:
                    return None
            summary = self._format_summary_locked(now)
            self._last_summary = summary
            self.last_render_at = now
            self._last_report_pages = self.pages_scanned
            self._dirty = False
            self._force_render = False
            return summary

    def render_summary(self) -> str:
        """Return the latest summary without throttling restrictions."""

        with self._lock:
            now = time.time()
            summary = self._format_summary_locked(now)
            self._last_summary = summary
            self.last_render_at = now
            self._last_report_pages = self.pages_scanned
            self._dirty = False
            self._force_render = False
            return summary

