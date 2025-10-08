from __future__ import annotations

import asyncio
from typing import Dict

_stop_event = asyncio.Event()
_tasks: Dict[str, asyncio.Task] = {}


def should_stop() -> bool:
    return _stop_event.is_set()


def request_stop() -> None:
    _stop_event.set()
    for task in list(_tasks.values()):
        if not task.done():
            task.cancel()


def clear_stop() -> None:
    if _stop_event.is_set():
        _stop_event.clear()


def register_task(name: str, task: asyncio.Task) -> None:
    _tasks[name] = task

    def _done(_task: asyncio.Task) -> None:
        _tasks.pop(name, None)

    task.add_done_callback(_done)


def running_tasks() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for name, task in _tasks.items():
        out[name] = f"done={task.done()} cancelled={task.cancelled()}"
    return out


def stop_and_status() -> dict:
    request_stop()
    return {"stopped": True, "running": running_tasks()}
