from __future__ import annotations

from pathlib import Path
import json
import os
from typing import Any, Dict, Iterable, Optional

from utils.paths import expand_path


def _allowed_exts() -> tuple[str, ...]:
    parts = [
        ext.strip().lower().lstrip(".")
        for ext in os.getenv("TEMPLATE_EXTS", "html,htm").split(",")
        if ext.strip()
    ]
    return tuple(f".{ext}" for ext in parts) or (".html", ".htm")


def _base_dir() -> Path:
    return expand_path(os.getenv("TEMPLATES_DIR", "templates"))


def _labels_path(base: Path) -> Path:
    return base / "_labels.json"


def _exclude_dirs(base: Path) -> list[Path]:
    raw = os.getenv("TEMPLATES_EXCLUDE", "templates/examples")
    dirs: list[Path] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        path = Path(part).expanduser()
        if not path.is_absolute():
            path = (base / path).resolve()
        else:
            path = path.resolve()
        dirs.append(path)
    return dirs


def _humanize(code: str) -> str:
    return code.replace("_", " ").replace("-", " ").strip().title()


def _load_labels(base: Path) -> Dict[str, Any]:
    labels_file = _labels_path(base)
    if not labels_file.exists():
        return {}
    try:
        data = json.loads(labels_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _normalize_code(code: str) -> str:
    return (code or "").strip().lower()


def _iter_template_files(base: Path) -> Iterable[Path]:
    if not base.exists() or not base.is_dir():
        return []
    excludes = _exclude_dirs(base)
    exts = _allowed_exts()
    files: list[Path] = []
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix not in exts:
            continue
        resolved = path.resolve()
        excluded = False
        for ex_dir in excludes:
            if not ex_dir.exists():
                continue
            try:
                resolved.relative_to(ex_dir)
            except ValueError:
                continue
            else:
                excluded = True
                break
        if excluded:
            continue
        files.append(resolved)
    return files


def list_templates() -> list[dict[str, Any]]:
    base = _base_dir()
    labels = _load_labels(base)
    templates: list[dict[str, Any]] = []
    for file_path in _iter_template_files(base):
        code = file_path.stem
        label_entry = labels.get(code)
        extra: Dict[str, Any]
        if isinstance(label_entry, dict):
            label = str(label_entry.get("label") or _humanize(code))
            extra = {k: v for k, v in label_entry.items() if k != "label"}
        elif isinstance(label_entry, str):
            label = label_entry
            extra = {}
        else:
            label = _humanize(code)
            extra = {}
        tpl: Dict[str, Any] = {"code": code, "label": label, "path": str(file_path)}
        tpl.update(extra)
        templates.append(tpl)
    return sorted(templates, key=lambda x: x.get("label", ""))


def get_template(code: str) -> dict[str, Any] | None:
    normalized = _normalize_code(code)
    if not normalized:
        return None
    for tpl in list_templates():
        if _normalize_code(tpl.get("code")) == normalized:
            return tpl
    return None


def get_template_label(code: str) -> str:
    """Return a human-readable label for template ``code``.

    Falls back to the template ``code`` when metadata is missing or incomplete.
    """

    if not code:
        return ""
    template: Optional[Dict[str, Any]] = get_template(code)
    if isinstance(template, dict):
        raw_label = template.get("label")
        if isinstance(raw_label, str):
            label = raw_label.strip()
            if label:
                return label
    fallback = str(code).strip()
    return fallback or str(code)


def get_template_by_path(path: str | Path) -> dict[str, Any] | None:
    try:
        resolved = Path(path).resolve()
    except Exception:
        return None
    for tpl in list_templates():
        try:
            tpl_path = Path(tpl.get("path", "")).resolve()
        except Exception:
            continue
        if tpl_path == resolved:
            return tpl
    return None
