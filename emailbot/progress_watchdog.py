"""Async heartbeat/watchdog helpers to detect parser hangs."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import faulthandler

__all__ = ["heartbeat", "heartbeat_now", "start_watchdog"]

_last_beat: float = 0.0
_lock = asyncio.Lock()


async def heartbeat() -> None:
    """Record a heartbeat timestamp from async code."""

    global _last_beat
    async with _lock:
        _last_beat = time.monotonic()


def heartbeat_now() -> None:
    """Record a heartbeat timestamp from synchronous/to_thread code."""

    global _last_beat
    _last_beat = time.monotonic()


async def start_watchdog(
    task: asyncio.Task[object],
    *,
    idle_seconds: float = 45.0,
    dump_path: str = "var/hang_dump.txt",
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
                    # Dump *all* threads to help investigate deadlocks.
                    faulthandler.dump_traceback(file=fh, all_threads=True)
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.error("Watchdog: dump failed: %r", exc)
            task.cancel("watchdog")
            break
    except asyncio.CancelledError:  # pragma: no cover - defensive cleanup
        pass
