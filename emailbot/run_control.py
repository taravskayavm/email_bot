from __future__ import annotations

import asyncio
from typing import Dict, Optional

_stop_event = asyncio.Event()
_tasks: Dict[str, asyncio.Task] = {}


def should_stop() -> bool:
    return _stop_event.is_set()


def request_stop() -> None:
    """Signal cooperative stop and cancel all registered tasks."""

    _stop_event.set()
    for task in list(_tasks.values()):
        if not task.done():
            task.cancel()
    _tasks.clear()


def clear_stop() -> None:
    """Reset the stop flag before starting a new long-running operation."""

    if _stop_event.is_set():
        _stop_event.clear()


def register_task(name: str, task: asyncio.Task) -> None:
    """Register ``task`` under ``name`` for cooperative cancellation."""

    old = _tasks.get(name)
    if old and old is not task and not old.done():
        old.cancel()
    _tasks[name] = task

    def _cleanup(finished: asyncio.Task) -> None:
        current = _tasks.get(name)
        if current is finished:
            _tasks.pop(name, None)

    task.add_done_callback(_cleanup)


def unregister_task(name: str) -> None:
    """Remove a previously registered task without cancelling it."""

    _tasks.pop(name, None)


def get_task(name: str) -> Optional[asyncio.Task]:
    """Return a registered task by ``name`` if it exists."""

    return _tasks.get(name)


def running_tasks() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name, task in _tasks.items():
        out[name] = f"done={task.done()} cancelled={task.cancelled()}"
    return out


def stop_and_status() -> dict:
    running = running_tasks()
    request_stop()
    return {"stopped": True, "running": running}
