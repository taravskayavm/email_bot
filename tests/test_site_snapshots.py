"""Smoke-tests for HTML snapshots captured with Playwright."""

from __future__ import annotations

from pathlib import Path

import pytest

from emailbot.extraction import extract_from_html_stream
from utils.tld_utils import is_allowed_domain


SITES_DIR = Path(__file__).resolve().parent / "fixtures/sites"


@pytest.mark.skipif(not SITES_DIR.exists(), reason="no site snapshots provided")
def test_snapshots_basic_quality() -> None:
    for html_path in sorted(SITES_DIR.glob("*.html")):
        data = html_path.read_bytes()
        hits, stats = extract_from_html_stream(data, source_ref=str(html_path))
        cleaned = {hit.email for hit in hits}
        assert len(cleaned) >= 0
        for email in cleaned:
            domain = email.split("@", 1)[1]
            assert is_allowed_domain(domain), f"{html_path}: bad domain {domain}"
        suspects = set(stats.get("emails_suspects") or stats.get("suspects") or [])
        assert cleaned.isdisjoint(suspects), f"{html_path} suspects leaked into cleaned"
