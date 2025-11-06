"""Global cancellation token utilities."""

from __future__ import annotations

from threading import Event

__all__ = ["cancel_all", "reset_all", "is_cancelled"]


_evt = Event()


def cancel_all() -> None:
    """Trigger the global cancellation token."""

    _evt.set()


def reset_all() -> None:
    """Reset the global cancellation token."""

    _evt.clear()


def is_cancelled() -> bool:
    """Return whether cancellation has been requested."""

    return _evt.is_set()
