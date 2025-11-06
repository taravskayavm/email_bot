"""Process-based timeout helpers."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, Optional

from multiprocessing.synchronize import Event as _Event

from emailbot.cancel_token import (
    get_shared_event,
    install_shared_event,
    is_cancelled,
)

__all__ = ["run_with_timeout"]

_POOL: Optional[ProcessPoolExecutor] = None


def _init_cancel_token(event: _Event) -> None:
    """Initializer for worker processes to install the shared cancellation token."""

    install_shared_event(event)


def _get_pool() -> ProcessPoolExecutor:
    global _POOL
    if _POOL is None:
        _POOL = ProcessPoolExecutor(
            max_workers=2,
            initializer=_init_cancel_token,
            initargs=(get_shared_event(),),
        )
    return _POOL


def run_with_timeout(func: Callable[..., Any], timeout: int, *args: Any, **kwargs: Any):
    """Execute ``func`` in a background process with a hard timeout."""

    if is_cancelled():
        return None

    pool = _get_pool()
    future = pool.submit(func, *args, **kwargs)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeout:
        try:
            future.cancel()
        except Exception:
            pass
        return None
    except Exception:
        return None
