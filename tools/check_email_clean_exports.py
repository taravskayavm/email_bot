"""Verify that all imports from utils.email_clean are satisfied."""

from __future__ import annotations

import ast
import importlib
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
TARGET_MODULE = "utils.email_clean"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def iter_python_files(root: pathlib.Path) -> Iterable[pathlib.Path]:
    for path in root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        yield path


def find_imported_names() -> list[str]:
    names: set[str] = set()
    for path in iter_python_files(ROOT):
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == TARGET_MODULE:
                for alias in node.names:
                    names.add(alias.name)
    return sorted(names)


def main() -> None:
    imported = find_imported_names()
    module = importlib.import_module(TARGET_MODULE)
    exports = set(module.__dict__.keys())
    missing = [name for name in imported if name not in exports]
    if missing:
        print(f"[FAIL] {TARGET_MODULE} missing {len(missing)} exported names:")
        for name in missing:
            print(f"  - {name}")
        raise SystemExit(1)
    print(f"[OK] {TARGET_MODULE}: all {len(imported)} imports satisfied")


if __name__ == "__main__":
    main()
