"""Helpers for parsing multiple local files concurrently."""

from __future__ import annotations

from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeout,
    as_completed,
)
import math
from typing import Any, Callable, Iterable, List

from emailbot.settings import PARSE_MAX_WORKERS, PARSE_FILE_TIMEOUT


def parallel_map_files(files: Iterable[str], worker: Callable[[str], Any]) -> List[Any]:
    """Apply ``worker`` to each path from ``files`` using a thread pool.

    Results are collected in completion order, so one slow file does not block
    already-finished files behind it. Exceptions and unfinished work after the
    batch deadline are ignored to avoid cancelling the whole batch.
    """

    file_list = [str(path) for path in files if str(path)]
    if not file_list:
        return []

    max_workers = max(1, PARSE_MAX_WORKERS)
    results: List[Any] = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {executor.submit(worker, path): path for path in file_list}
        # PARSE_FILE_TIMEOUT historically applied while awaiting every future
        # separately. Use the number of worker waves as a comparable batch
        # deadline while allowing completed futures to be consumed at once.
        timeout = None
        if PARSE_FILE_TIMEOUT > 0:
            waves = max(1, math.ceil(len(file_list) / max_workers))
            timeout = PARSE_FILE_TIMEOUT * waves
        try:
            for future in as_completed(futures, timeout=timeout):
                try:
                    results.append(future.result())
                except Exception:
                    continue
        except FuturesTimeout:
            for future in futures:
                if not future.done():
                    future.cancel()
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return results
