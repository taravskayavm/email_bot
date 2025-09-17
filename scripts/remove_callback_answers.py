#!/usr/bin/env python3
"""
Comment-out all CallbackQuery.answer(...) calls across the repo to avoid Telegram
callback "answer" usage. We only touch *callback* answers:
  - await update.callback_query.answer(...)
  - await query.answer(...)            where `query = update.callback_query`
  - await callback_query.answer(...)
We do NOT touch inline/pre-checkout/shipping query answers.
Idempotent: re-running won't duplicate comments.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(".").resolve()
SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "env",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
}

# 1) var binding to update.callback_query
ASSIGN_CBQ_RE = re.compile(r'^\s*([A-Za-z_]\w*)\s*=\s*update\.callback_query\b')
# 2) direct call on update.callback_query
DIRECT_CBQ_ANSWER_RE = re.compile(r'^\s*(await\s+)?update\.callback_query\s*\.\s*answer\s*\(')
# 3) generic var call .answer( ... )  -> we filter by known var names bound to callback_query
VAR_ANSWER_RE = re.compile(r'^\s*(await\s+)?([A-Za-z_]\w*)\s*\.\s*answer\s*\(')

MARK = "# removed by codex: callback_query.answer()"


def iter_py_files(root: Path):
    for path in root.rglob("*.py"):
        if set(path.parts) & SKIP_DIRS:
            continue
        yield path


def process_file(path: Path) -> bool:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=False)
    changed = False

    # First pass: collect variable names bound to update.callback_query
    cbq_vars: set[str] = set()
    for line in lines:
        match = ASSIGN_CBQ_RE.match(line)
        if match:
            cbq_vars.add(match.group(1))

    # Always treat common names as callback query variables
    cbq_vars |= {"query", "callback_query"}

    new_lines: list[str] = []
    for line in lines:
        raw = line
        if MARK in line:
            new_lines.append(line)
            continue

        # a) direct: update.callback_query.answer(...)
        if DIRECT_CBQ_ANSWER_RE.match(line):
            indent = len(line) - len(line.lstrip(" "))
            prefix = line[:indent]
            line = f"{prefix}{MARK}  {raw.strip()}"
            changed = True
        else:
            # b) var: <name>.answer(...), only if <name> is a known callback_query alias
            match = VAR_ANSWER_RE.match(line)
            if match:
                var_name = match.group(2)
                if var_name in cbq_vars:
                    indent = len(line) - len(line.lstrip(" "))
                    prefix = line[:indent]
                    line = f"{prefix}{MARK}  {raw.strip()}"
                    changed = True

        new_lines.append(line)

    if changed:
        path.write_text("\n".join(new_lines) + ("\n" if text.endswith("\n") else ""), encoding="utf-8")
    return changed


def main() -> None:
    changed_any = False
    for py_file in iter_py_files(ROOT):
        if process_file(py_file):
            print(f"[patched] {py_file}")
            changed_any = True

    if not changed_any:
        print("No CallbackQuery.answer calls found or already removed.")


if __name__ == "__main__":
    main()
