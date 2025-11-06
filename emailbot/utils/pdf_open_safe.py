"""Utilities for probing PDF files with a strict timeout."""

from __future__ import annotations

import multiprocessing as mp
import time
from typing import Optional, Tuple


def _open_worker(path: str, q: mp.Queue) -> None:
    try:
        import fitz  # type: ignore  # PyMuPDF

        doc = fitz.open(path)
        try:
            pages = len(doc)
        except Exception:
            pages = 0
        finally:
            try:
                doc.close()
            except Exception:
                pass
        q.put(("ok", pages))
    except Exception as exc:  # pragma: no cover - depends on runtime env
        q.put(("err", repr(exc)))


def open_pdf_with_timeout(path: str, timeout_sec: float) -> Tuple[bool, Optional[int], Optional[str]]:
    """Attempt to open a PDF via PyMuPDF in a helper process with a timeout.

    Returns a tuple ``(ok, pages, error)``. ``ok`` is ``True`` when ``fitz``
    managed to open the document and report the number of pages within the
    provided timeout. When the timeout is hit ``ok`` is ``False`` and ``error``
    is set to ``"timeout"``.
    """

    q: mp.Queue = mp.Queue(maxsize=1)
    proc = mp.Process(target=_open_worker, args=(path, q), daemon=True)
    proc.start()
    start = time.time()

    while proc.is_alive():
        if time.time() - start > timeout_sec:
            try:
                proc.terminate()
            finally:
                return False, None, "timeout"
        time.sleep(0.02)

    if not q.empty():
        tag, payload = q.get()
        if tag == "ok":
            return True, int(payload), None
        return False, None, payload

    return False, None, "unknown"
