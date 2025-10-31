"""Async heartbeat/watchdog helpers to detect parser hangs."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import TypedDict

import faulthandler

__all__ = [
    "heartbeat",
    "heartbeat_now",
    "start_watchdog",
    "start_heartbeat_pulse",
    "ProgressTracker",
    "ProgressWatchdog",
]

_last_beat: float = 0.0
_lock = asyncio.Lock()


class _ProgressSnapshot(TypedDict, total=False):
    """Typed representation of :class:`ProgressTracker` snapshots."""

    last_progress: float
    files_total: int
    files_processed: int
    files_skipped: int
    last_file: str


class ProgressTracker:
    """Track long-running job progress in a thread-safe way.

    The tracker is intended to be shared between worker threads that process
    files and watchdogs that monitor for stalls.  ``tick_file`` should be
    called once a file processing attempt finishes (regardless of success) to
    record progress.  ``reset`` updates counters when a new batch starts.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_progress = time.monotonic()
        self._files_total = 0
        self._files_processed = 0
        self._files_skipped = 0
        self._last_file = ""

    def reset(self, *, total: int | None = None) -> None:
        """Reset counters for a new batch of work."""

        with self._lock:
            if total is not None:
                self._files_total = max(int(total), 0)
            self._files_processed = 0
            self._files_skipped = 0
            self._last_file = ""
            self._last_progress = time.monotonic()

    def reset_total(self, total: int) -> None:
        """Reset the known total number of files in the batch."""

        self.reset(total=total)

    def extend_total(self, count: int) -> None:
        """Increase the total number of files without resetting progress."""

        if count <= 0:
            return
        with self._lock:
            self._files_total += int(count)

    def tick_file(self, filename: str, *, processed: bool = True) -> None:
        """Mark ``filename`` as processed and refresh the progress timestamp."""

        now = time.monotonic()
        with self._lock:
            if processed:
                self._files_processed += 1
            else:
                self._files_skipped += 1
            self._last_file = filename
            self._last_progress = now

    def snapshot(self) -> _ProgressSnapshot:
        """Return a shallow copy of the current progress state."""

        with self._lock:
            return _ProgressSnapshot(
                last_progress=self._last_progress,
                files_total=self._files_total,
                files_processed=self._files_processed,
                files_skipped=self._files_skipped,
                last_file=self._last_file,
            )


class ProgressWatchdog:
    """Synchronous watchdog that inspects :class:`ProgressTracker` state."""

    def __init__(
        self,
        tracker: ProgressTracker,
        *,
        idle_seconds: float = 90.0,
        dump_path: str = "var/hang_dump.txt",
    ) -> None:
        self._tracker = tracker
        self._idle_seconds = float(idle_seconds)
        self._dump_path = dump_path
        self._stopped = False

    def stop(self) -> None:
        """Disable the watchdog."""

        self._stopped = True

    def check_or_raise(self) -> None:
        """Raise ``TimeoutError`` if no progress has been observed recently."""

        if self._stopped:
            return
        snapshot = self._tracker.snapshot()
        last_progress = snapshot.get("last_progress", 0.0) or 0.0
        if time.monotonic() - float(last_progress) <= self._idle_seconds:
            return
        try:
            Path(self._dump_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self._dump_path, "w", encoding="utf-8") as fh:
                fh.write(
                    f"no file progress for >{self._idle_seconds:.0f}s\n"
                )
                files_total = int(snapshot.get("files_total") or 0)
                files_processed = int(snapshot.get("files_processed") or 0)
                files_skipped = int(snapshot.get("files_skipped") or 0)
                fh.write(
                    f"files_processed={files_processed}/{files_total}" "\n"
                )
                if files_skipped:
                    fh.write(f"files_skipped={files_skipped}\n")
                last_file = snapshot.get("last_file")
                if last_file:
                    fh.write(f"last_file={last_file}\n")
        except Exception:  # pragma: no cover - best effort logging
            pass
        raise TimeoutError("no progress")


async def heartbeat() -> None:
    """Record a heartbeat timestamp from async code."""

    global _last_beat
    async with _lock:
        _last_beat = time.monotonic()


def heartbeat_now() -> None:
    """Record a heartbeat timestamp from synchronous/to_thread code."""

    global _last_beat
    _last_beat = time.monotonic()


async def _heartbeat_pulse(interval: float = 5.0) -> None:
    """Emit periodic heartbeats while awaiting long-running operations."""

    try:
        while True:
            await asyncio.sleep(max(interval, 0.1))
            await heartbeat()
    except asyncio.CancelledError:  # pragma: no cover - cooperative cancellation
        pass


def start_heartbeat_pulse(*, interval: float = 5.0) -> asyncio.Task[None]:
    """Start a background task that periodically emits heartbeats."""

    loop = asyncio.get_running_loop()
    return loop.create_task(_heartbeat_pulse(interval))


async def start_watchdog(
    task: asyncio.Task[object],
    *,
    idle_seconds: float = 45.0,
    dump_path: str = "var/hang_dump.txt",
    tracker: ProgressTracker | None = None,
) -> None:
    """Monitor ``task`` and cancel it if no heartbeat is observed."""

    global _last_beat

    Path(dump_path).parent.mkdir(parents=True, exist_ok=True)
    _last_beat = time.monotonic()

    try:
        while not task.done():
            await asyncio.sleep(1.0)
            idle = time.monotonic() - _last_beat
            if idle < idle_seconds:
                continue
            logging.error(
                f"Watchdog: no progress for {float(idle):.1f}s, cancelling task…"
            )
            try:
                with open(dump_path, "w", encoding="utf-8") as fh:
                    fh.write(f"=== HANG DUMP (idle {idle:.1f}s) ===\n")
                    fh.write(f"Task: {task!r}\n")
                    if tracker is not None:
                        snapshot = tracker.snapshot()
                        files_total = int(snapshot.get("files_total") or 0)
                        files_processed = int(snapshot.get("files_processed") or 0)
                        files_skipped = int(snapshot.get("files_skipped") or 0)
                        fh.write(
                            "Progress: "
                            f"{files_processed}/{files_total} processed"
                        )
                        if files_skipped:
                            fh.write(f", {files_skipped} skipped")
                        fh.write("\n")
                        last_file = snapshot.get("last_file")
                        if last_file:
                            fh.write(f"Last file: {last_file}\n")
                    # Dump *all* threads to help investigate deadlocks.
                    faulthandler.dump_traceback(file=fh, all_threads=True)
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.error("Watchdog: dump failed: %r", exc)
            task.cancel("watchdog")
            break
    except asyncio.CancelledError:  # pragma: no cover - defensive cleanup
        pass
