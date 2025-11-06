"""Process-based timeout helpers."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, Optional

__all__ = ["run_with_timeout"]

_POOL: Optional[ProcessPoolExecutor] = None


def _get_pool() -> ProcessPoolExecutor:
    global _POOL
    if _POOL is None:
        _POOL = ProcessPoolExecutor(max_workers=2)
    return _POOL


def run_with_timeout(func: Callable[..., Any], timeout: int, *args: Any, **kwargs: Any):
    """Execute ``func`` in a background process with a hard timeout."""

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
