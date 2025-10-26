"""API contract tests for utils.email_clean and key modules."""

import importlib
from typing import Iterable

import pytest

REQUIRED_EXPORTS = {
    "dedupe_with_variants",
    "dedupe_keep_original",
    "canonical_email",
    "parse_emails_unified",
    "sanitize_email",
    "finalize_email",
    "normalize_email",
    "repair_email",
    "get_variants",
    "drop_leading_char_twins",
    "drop_trailing_char_twins",
}


@pytest.mark.parametrize(
    "module_name",
    [
        "utils.email_clean",
        "emailbot",
        "emailbot.bot_handlers",
        "pipelines",
    ],
)
def test_importable_modules(module_name: str) -> None:
    """Ensure key modules remain importable."""
    try:
        importlib.import_module(module_name)
    except Exception as exc:  # pragma: no cover - failure path
        pytest.fail(f"Module {module_name} failed to import: {exc}")


def test_email_clean_exports_complete() -> None:
    """Ensure utils.email_clean exposes the full public API contract."""
    module = importlib.import_module("utils.email_clean")
    missing = sorted(_missing_exports(module, REQUIRED_EXPORTS))
    if missing:
        pytest.fail(f"utils.email_clean missing exports: {missing}")


def _missing_exports(module: object, required: Iterable[str]) -> list[str]:
    """Return list of required export names that are absent on module."""
    return [name for name in required if not hasattr(module, name)]
