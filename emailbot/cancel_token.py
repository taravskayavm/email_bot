"""Global cancellation token utilities."""

from __future__ import annotations

from multiprocessing import Event as _MPEvent
from multiprocessing.synchronize import Event as _Event

__all__ = [
    "cancel_all",
    "reset_all",
    "is_cancelled",
    "get_shared_event",
    "install_shared_event",
]


_evt: _Event = _MPEvent()


def install_shared_event(event: _Event) -> None:
    """Install a shared multiprocessing event used across worker processes."""

    global _evt
    _evt = event


def get_shared_event() -> _Event:
    """Return the shared multiprocessing event backing the cancellation token."""

    return _evt


def cancel_all() -> None:
    """Trigger the global cancellation token."""

    _evt.set()


def reset_all() -> None:
    """Reset the global cancellation token."""

    _evt.clear()


def is_cancelled() -> bool:
    """Return whether cancellation has been requested."""

    return _evt.is_set()
