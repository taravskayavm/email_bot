"""Helpers for parsing multiple local files concurrently."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, Iterable, List

from emailbot.settings import PARSE_MAX_WORKERS, PARSE_FILE_TIMEOUT


def parallel_map_files(files: Iterable[str], worker: Callable[[str], Any]) -> List[Any]:
    """Apply ``worker`` to each path from ``files`` using a thread pool.

    Each worker call is bounded by :data:`emailbot.settings.PARSE_FILE_TIMEOUT`.
    Exceptions (including timeouts) are ignored to avoid cancelling the whole
    batch; only successful results are returned.
    """

    file_list = [str(path) for path in files if str(path)]
    if not file_list:
        return []

    max_workers = max(1, PARSE_MAX_WORKERS)
    results: List[Any] = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {executor.submit(worker, path): path for path in file_list}
        for future, path in futures.items():
            try:
                if PARSE_FILE_TIMEOUT > 0:
                    value = future.result(timeout=PARSE_FILE_TIMEOUT)
                else:
                    value = future.result()
            except FuturesTimeout:
                future.cancel()
                continue
            except Exception:
                continue
            results.append(value)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return results
