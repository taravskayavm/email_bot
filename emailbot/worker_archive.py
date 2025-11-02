"""Helper utilities for running archive parsing in a separate process."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple
import json
import logging
import multiprocessing as mp
import os
import time
import traceback
import uuid
from pathlib import Path

from .progress_watchdog import ProgressTracker


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
            _atomic_write_json(progress_path, {"progress": dict(snapshot)})
        except Exception:  # pragma: no cover - best effort delivery
            pass

    tracker = ProgressTracker(on_update=_emit_progress)

    # Bootstrap-прогресс: чтобы вотчдог увидел «жизнь» сразу после старта
    try:
        _emit_progress({"stage": "boot", "done": 0, "total": None})
    except Exception:
        pass

    try:
        emails, stats = _extraction.extract_any(zip_path, tracker=tracker)

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
                {"stage": "finalize", "done": processed, "total": total}
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


def run_parse_in_subprocess(
    zip_path: str,
    timeout_sec: int,
    *,
    progress_callback: Callable[[Dict[str, Any]], None] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    """Run ZIP parsing in a forked process with a hard timeout (Windows-safe)."""

    # На Windows spawn может ломаться из-за инициализации пакета.
    # Пробуем "forkserver" если доступен, иначе spawn, но через стартовый модуль без import emailbot.*
    try:
        ctx = mp.get_context("forkserver")
    except ValueError:
        ctx = mp.get_context("spawn")

    # Абсолютная директория для артефактов, чтобы не зависеть от CWD подпроцесса
    base_dir = Path(__file__).resolve().parent.parent
    var_dir = base_dir / "var"
    var_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    out_json_path = str(var_dir / f"worker_result_{token}.json")
    progress_path = (
        str(var_dir / f"worker_progress_{token}.json")
        if progress_callback is not None
        else None
    )
    process = ctx.Process(
        target=_worker,
        args=(zip_path, out_json_path, progress_path),
        daemon=True,
    )

    # Убираем привязку к контексту telegram/logging в подпроцессе
    process.daemon = False
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

    while time.monotonic() < deadline:
        if progress_path and os.path.exists(progress_path):
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

    _cleanup_artifacts(progress_path, out_json_path)

    if not data.get("ok"):
        return False, {
            "error": data.get("error", "unknown error"),
            "traceback": data.get("traceback"),
        }

    stats = data.get("stats") or {}
    if isinstance(stats, dict):
        _notify_progress(stats)

    return True, {"emails": data["emails"], "stats": stats}


__all__ = ["run_parse_in_subprocess"]
