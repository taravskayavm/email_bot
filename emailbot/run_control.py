from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Dict, List, Optional

from emailbot.cancel_token import cancel_all as _cancel_all, reset_all as _reset_all, is_cancelled as _is_cancelled
from .utils.logging_setup import get_logger

_stop_event = asyncio.Event()
_tasks: Dict[str, asyncio.Task] = {}
_executors: List[ThreadPoolExecutor] = []
_executor_lock = Lock()

logger = get_logger(__name__)


def should_stop() -> bool:
    return _stop_event.is_set() or _is_cancelled()


def request_stop() -> None:
    """Signal cooperative stop and cancel all registered tasks."""

    _stop_event.set()
    _cancel_all()
    for task in list(_tasks.values()):
        if not task.done():
            task.cancel()
    _tasks.clear()

    with _executor_lock:
        for executor in list(_executors):
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                logger.warning("executor shutdown failed", exc_info=True)
        _executors.clear()


def clear_stop() -> None:
    """Reset the stop flag before starting a new long-running operation."""

    if _stop_event.is_set():
        _stop_event.clear()
    _reset_all()


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


def unregister_task(name: str, task: Optional[asyncio.Task] = None) -> None:
    """Remove a previously registered task without cancelling it."""

    current = _tasks.get(name)
    if task is None or current is task:
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


def register_executor(executor: ThreadPoolExecutor) -> None:
    """Track ``executor`` so it can be cancelled on stop."""

    with _executor_lock:
        _executors.append(executor)


def unregister_executor(executor: ThreadPoolExecutor) -> None:
    """Remove ``executor`` from tracking list if present."""

    with _executor_lock:
        try:
            _executors.remove(executor)
        except ValueError:
            pass


def clear_executors() -> None:
    """Forget about registered executors without shutting them down."""

    with _executor_lock:
        _executors.clear()
