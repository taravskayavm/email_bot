"""Helper utilities for running archive parsing in a separate process."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple
import queue
import json
import logging
import multiprocessing as mp
import os
import threading
import time
import traceback
import uuid
from pathlib import Path

from .progress_watchdog import ProgressTracker, heartbeat_now


logger = logging.getLogger(__name__)


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    tmp = f"{path}.part"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp, path)


def _load_json(path: str) -> Dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # pragma: no cover - best effort read
        return None


def _remove_quiet(path: os.PathLike[str] | str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except OSError:  # pragma: no cover - best effort cleanup
        pass


def _cleanup_artifacts(*paths: os.PathLike[str] | str | None) -> None:
    for path in paths:
        if not path:
            continue
        _remove_quiet(path)
        _remove_quiet(f"{path}.part")


def _normalize_progress_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize progress snapshot keys for UI/backwards compatibility."""

    snap = dict(snapshot)
    if "processed" in snap and "done" not in snap:
        snap["done"] = snap["processed"]
    if "done" in snap and "processed" not in snap:
        snap["processed"] = snap["done"]
    if "current" in snap and "file" not in snap:
        snap["file"] = snap["current"]
    return snap


def _worker(zip_path: str, out_json_path: str, progress_path: str | None) -> None:
    """Worker process entry point."""

    # Ленивый импорт тяжёлых модулей, чтобы spawn не падал на импортёрах.
    try:
        from . import extraction as _extraction  # lazy import
    except Exception as exc:  # pragma: no cover - defensive
        tb = traceback.format_exc()
        try:
            _atomic_write_json(
                out_json_path,
                {"ok": False, "error": f"import failed: {exc}", "traceback": tb},
            )
        except Exception:
            pass
        return

    def _emit_progress(snapshot: Dict[str, Any]) -> None:
        if progress_path is None:
            return
        try:
            snap = _normalize_progress_snapshot(snapshot)
            _atomic_write_json(progress_path, {"progress": snap})
        except Exception:  # pragma: no cover - best effort delivery
            pass

    tracker = ProgressTracker(on_update=_emit_progress)

    # Bootstrap-прогресс: чтобы вотчдог увидел «жизнь» сразу после старта
    try:
        _emit_progress({"stage": "worker_boot", "processed": 0, "total": None})
    except Exception:
        pass

    try:
        try:
            _emit_progress({"stage": "route_start", "processed": 0, "total": None})
        except Exception:
            pass

        emails, stats = _extraction.extract_any(zip_path, tracker=tracker)

        try:
            _emit_progress(
                {
                    "stage": "route_done",
                    "processed": stats.get("processed", 0),
                    "total": stats.get("total", None),
                }
            )
        except Exception:
            pass

        # На случай, если парсер ни разу не дёрнул tracker (пустой архив/всё отфильтровано)
        try:
            processed = (
                stats.get("files_processed")
                or stats.get("processed")
                or stats.get("done")
                or 0
            )
            total = (
                stats.get("files_total")
                or stats.get("total")
                or stats.get("expected")
                or None
            )
            _emit_progress(
                {"stage": "finalize", "processed": processed, "total": total}
            )
        except Exception:
            pass
        _atomic_write_json(
            out_json_path,
            {"ok": True, "emails": emails, "stats": stats},
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        tb = traceback.format_exc()
        # Ensure we use the standard logging API to avoid deprecated aliases.
        logger.warning("zip worker failed: %s", exc)
        try:
            _atomic_write_json(
                out_json_path,
                {"ok": False, "error": str(exc), "traceback": tb},
            )
        except Exception:  # pragma: no cover - best effort delivery
            pass


def _thread_worker(
    zip_path: str,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None,
) -> Tuple[bool, Dict[str, Any]]:
    """Thread worker variant that delivers progress via callbacks."""

    try:
        from . import extraction as _extraction  # lazy import
    except Exception as exc:  # pragma: no cover - defensive
        tb = traceback.format_exc()
        return False, {"error": f"import failed: {exc}", "traceback": tb}

    def _emit_progress(snapshot: Dict[str, Any]) -> None:
        try:
            heartbeat_now()
        except Exception:  # pragma: no cover - best effort heartbeat
            pass
        if progress_callback is None:
            return
        try:
            progress_callback(_normalize_progress_snapshot(snapshot))
        except Exception:  # pragma: no cover - defensive notification
            logger.debug("progress callback failed", exc_info=True)

    tracker = ProgressTracker(on_update=_emit_progress)

    try:
        try:
            _emit_progress({"stage": "worker_boot", "processed": 0, "total": None})
        except Exception:  # pragma: no cover - best effort delivery
            pass

        try:
            _emit_progress({"stage": "route_start", "processed": 0, "total": None})
        except Exception:  # pragma: no cover - best effort delivery
            pass

        emails, stats = _extraction.extract_any(zip_path, tracker=tracker)

        try:
            _emit_progress(
                {
                    "stage": "route_done",
                    "processed": stats.get("processed", 0),
                    "total": stats.get("total", None),
                }
            )
        except Exception:  # pragma: no cover - best effort delivery
            pass

        processed = (
            stats.get("files_processed")
            or stats.get("processed")
            or stats.get("done")
            or 0
        )
        total = (
            stats.get("files_total")
            or stats.get("total")
            or stats.get("expected")
            or None
        )
        try:
            _emit_progress(
                {"stage": "finalize", "processed": processed, "total": total}
            )
        except Exception:  # pragma: no cover - best effort delivery
            pass

        return True, {"emails": emails, "stats": stats}
    except Exception as exc:  # pragma: no cover - defensive fallback
        tb = traceback.format_exc()
        logger.warning("zip worker failed: %s", exc)
        return False, {"error": str(exc), "traceback": tb}


def _run_parse_via_thread(
    zip_path: str,
    timeout_sec: int,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a background thread (Windows fallback)."""

    result_queue: "queue.SimpleQueue[Tuple[bool, Dict[str, Any]]]" = queue.SimpleQueue()
    state_lock = threading.Lock()
    last_snapshot: Dict[str, Any] | None = None
    heartbeat_interval = 5.0
    next_heartbeat = time.monotonic() + heartbeat_interval

    def _deliver_progress(snapshot: Dict[str, Any]) -> None:
        callback = progress_callback
        if callback is None:
            return
        try:
            callback(snapshot)
        except Exception:  # pragma: no cover - defensive notification
            logger.debug("progress callback failed", exc_info=True)

    def _progress_wrapper(snapshot: Dict[str, Any]) -> None:
        nonlocal last_snapshot, next_heartbeat
        now = time.monotonic()
        with state_lock:
            last_snapshot = dict(snapshot)
        next_heartbeat = now + heartbeat_interval
        _deliver_progress(snapshot)

    def _target() -> None:
        result = _thread_worker(zip_path, progress_callback=_progress_wrapper)
        try:
            result_queue.put(result)
        except Exception:  # pragma: no cover - best effort delivery
            logger.debug("thread worker result delivery failed", exc_info=True)

    thread = threading.Thread(target=_target, name="zip-worker-thread", daemon=True)
    thread.start()

    deadline = time.monotonic() + timeout_sec

    try:
        while True:
            try:
                return result_queue.get_nowait()
            except queue.Empty:
                pass

            now = time.monotonic()

            if now >= deadline:
                try:
                    heartbeat_now()
                except Exception:  # pragma: no cover - best effort heartbeat
                    pass
                timeout_payload = _normalize_progress_snapshot(
                    {"stage": "timeout", "processed": 0, "total": 0}
                )
                _deliver_progress(timeout_payload)
                logger.error("zip worker thread timed out after %ss", timeout_sec)
                return False, {"error": f"timeout after {timeout_sec}s"}

            if now >= next_heartbeat:
                try:
                    heartbeat_now()
                except Exception:  # pragma: no cover - best effort heartbeat
                    pass
                heartbeat_snapshot: Dict[str, Any]
                with state_lock:
                    heartbeat_snapshot = dict(last_snapshot or {})
                heartbeat_snapshot["stage"] = "heartbeat"
                heartbeat_payload = _normalize_progress_snapshot(heartbeat_snapshot)
                _deliver_progress(heartbeat_payload)
                next_heartbeat = now + heartbeat_interval

            if not thread.is_alive():
                break

            time.sleep(0.05)
    finally:
        try:
            if thread.is_alive():
                thread.join(timeout=0.1)
        except Exception:  # pragma: no cover - best effort cleanup
            pass

    try:
        return result_queue.get_nowait()
    except queue.Empty:
        logger.error("zip worker thread finished without returning a result")
        return False, {"error": "no result from thread"}


def _run_parse_via_process(
    zip_path: str,
    timeout_sec: int,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a forked process with a hard timeout."""

    # Явно используем spawn-контекст, чтобы не наследовать состояние родителя.
    try:
        ctx = mp.get_context("spawn")
    except ValueError:  # pragma: no cover - spawn обязан быть, но подстрахуемся
        ctx = mp.get_context()

    # Абсолютная директория для артефактов, чтобы не зависеть от CWD подпроцесса
    base_dir = Path(__file__).resolve().parent.parent
    var_dir = base_dir / "var"
    var_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    out_json_path = str(var_dir / f"worker_result_{token}.json")
    # Прогресс-файл ведём всегда — это источник правды для watchdog.
    progress_path = str(var_dir / f"worker_progress_{token}.json")
    process = ctx.Process(
        target=_worker,
        args=(zip_path, out_json_path, progress_path),
        name="zip-worker",
        daemon=True,
    )

    # Сбрасываем authkey, чтобы подпроцесс не наследовал токен PTB.
    process.authkey = b""

    logger.info("Starting zip worker via %s context", ctx.get_start_method())

    def _notify_progress(payload: Dict[str, Any]) -> None:
        if progress_callback is None:
            return
        try:
            progress_callback(dict(payload))
        except Exception:  # pragma: no cover - defensive
            logger.debug("progress callback failed", exc_info=True)

    try:
        process.start()
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("failed to start zip worker: %s", exc)
        return False, {"error": f"failed to start subprocess: {exc}"}

    deadline = time.monotonic() + timeout_sec
    data: Dict[str, Any] | None = None
    last_progress_mtime: float | None = None
    started = time.monotonic()
    parent_heartbeat_at = 0.0

    # [EB-FIX] сразу дёрнем глобальный «пульс» — watchdog увидит активность
    try:
        heartbeat_now()
    except Exception:
        pass

    while time.monotonic() < deadline:
        if os.path.exists(progress_path):
            try:
                mtime = os.path.getmtime(progress_path)
            except OSError:
                mtime = None
            if mtime and (last_progress_mtime is None or mtime > last_progress_mtime):
                try:
                    with open(progress_path, "r", encoding="utf-8") as fh:
                        payload = json.load(fh)
                except Exception:  # pragma: no cover - best effort read
                    payload = None
                if isinstance(payload, dict):
                    snapshot = payload.get("progress")
                    if isinstance(snapshot, dict):
                        _notify_progress(snapshot)
                        last_progress_mtime = mtime

        # [EB-FIX] Родительский heartbeat раз в ~1с, чтобы watchdog видел активность
        now = time.monotonic()
        if now - parent_heartbeat_at >= 1.0:
            # Сначала обновляем глобальный пульс для watchdog
            try:
                heartbeat_now()
            except Exception:
                pass
            try:
                if not os.path.exists(progress_path):
                    with open(progress_path, "w", encoding="utf-8") as fh:
                        json.dump(
                            {
                                "progress": {
                                    "stage": "parent_wait",
                                    "t": now,
                                    # [EB-FIX] добавим done/processed/total, чтобы UI не залипал на 0/3
                                    "done": 0,
                                    "processed": 0,
                                    "total": 0,
                                }
                            },
                            fh,
                        )
                else:
                    os.utime(progress_path, None)
            except Exception:
                pass
            parent_heartbeat_at = now

        if os.path.exists(out_json_path):
            data = _load_json(out_json_path)
            if data is not None:
                break

        if not process.is_alive():
            for _ in range(10):
                if os.path.exists(out_json_path):
                    data = _load_json(out_json_path)
                    break
                time.sleep(0.2)
            if data is None:
                elapsed = max(0.0, time.monotonic() - started)
                logger.error(
                    "zip worker exited prematurely (%.2fs) without output", elapsed
                )
                _cleanup_artifacts(progress_path, out_json_path)
                return False, {"error": "worker exited prematurely (no output)"}
            break

        time.sleep(0.2)

    if data is None:
        timed_out = time.monotonic() >= deadline
        try:
            if process.is_alive():
                process.terminate()
                process.join(2.0)
                if process.is_alive() and hasattr(process, "kill"):
                    process.kill()
        except Exception:  # pragma: no cover - defensive
            pass

        if timed_out:
            _cleanup_artifacts(progress_path, out_json_path)
            return False, {"error": f"timeout after {timeout_sec}s"}

        try:
            process.join(timeout=0.0)
        except Exception:  # pragma: no cover - defensive
            pass
        _cleanup_artifacts(progress_path, out_json_path)
        return False, {"error": "no result from subprocess"}

    if process.is_alive():
        try:
            process.join(0.1)
        except Exception:  # pragma: no cover - defensive
            pass

    exitcode = process.exitcode

    _cleanup_artifacts(progress_path, out_json_path)

    if exitcode not in (0, None):
        return False, {"error": f"zip worker exited with code {exitcode}"}

    if not data.get("ok"):
        return False, {
            "error": data.get("error", "unknown error"),
            "traceback": data.get("traceback"),
        }

    stats = data.get("stats") or {}
    if isinstance(stats, dict):
        processed = (
            stats.get("files_processed")
            or stats.get("processed")
            or stats.get("done")
            or 0
        )
        total = (
            stats.get("files_total")
            or stats.get("total")
            or stats.get("expected")
            or None
        )
        snapshot = dict(stats)
        snapshot.setdefault("stage", "finalize")
        snapshot.setdefault("processed", processed)
        if total is not None and "total" not in snapshot:
            snapshot["total"] = total
        _notify_progress(snapshot)

    return True, {"emails": data["emails"], "stats": stats}


def run_parse_in_subprocess(
    zip_path: str,
    timeout_sec: int,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing using the most reliable strategy for the platform."""

    if os.name == "nt":
        # --- [EBOT-THREAD-FALLBACK] на Windows используем поток, а не подпроцесс ---
        return _run_parse_via_thread(
            zip_path,
            timeout_sec,
            progress_callback=progress_callback,
        )
    return _run_parse_via_process(
        zip_path,
        timeout_sec,
        progress_callback=progress_callback,
    )


__all__ = ["run_parse_in_subprocess"]
