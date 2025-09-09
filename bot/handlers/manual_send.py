"""Utilities for manual e-mail sending.

This module re-exports :func:`parse_manual_input` from :mod:`utils.email_clean`
to provide a stable import location for callers.
"""

from __future__ import annotations

from utils.email_clean import parse_manual_input


__all__ = ["parse_manual_input"]

