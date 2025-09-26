# -*- coding: utf-8 -*-
"""
Quick smoke test for the e-mail extraction pipeline.
Checks:
  1) no mixed-script / 'poccия' / 'russia1...' glued garbage in final list
  2) allowed TLDs only (.ru, .com) unless overridden
  3) presence of real author e-mails when provided via --expect
Also prints which modules were actually imported (to catch duplicate files).
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import pathlib
import re
import sys
from typing import Iterable, Sequence

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from emailbot import extraction as extraction_module
from utils.tld_utils import is_foreign_domain

MIXED_BAD_RE = re.compile(r"poccия|росс[i|l]ya|russia\d", re.I)


def _normalize_email(value: object) -> str:
    try:
        text = str(value or "").strip()
    except Exception:  # pragma: no cover - defensive
        return ""
    return text.lower()


def extract_from_uploaded_file(path: str) -> tuple[set[str], set[str], dict]:
    emails_raw, stats = extraction_module.extract_any(path)
    normalized = {_normalize_email(email) for email in emails_raw if email}
    normalized.discard("")
    return normalized, set(normalized), stats


async def async_extract_emails_from_url(
    url: str,
    _session,  # pragma: no cover - compatibility placeholder
    _tag: str | None = None,
):
    """Asynchronously extract e-mails from ``url`` using the core extraction module."""

    def _run() -> tuple[str, set[str], set[str], list, dict]:
        hits, stats = extraction_module.extract_from_url(url)
        allowed = {_normalize_email(getattr(hit, "email", "")) for hit in hits}
        allowed.discard("")
        foreign = {addr for addr in allowed if is_foreign_domain(addr.rsplit("@", 1)[-1])}
        return url, allowed, foreign, [], stats

    return await asyncio.to_thread(_run)


def print_modules() -> None:
    modules = [
        "emailbot.extraction",
        "utils.email_clean",
        "utils.email_deobfuscate",
        "services.cooldown",
        "emailbot.bot.__main__",
    ]
    for name in modules:
        try:
            mod = importlib.import_module(name)
        except Exception as exc:  # pragma: no cover - debugging aid
            print(f"[module] {name}: <not-imported> ({exc})")
            continue
        location = getattr(mod, "__file__", None)
        print(f"[module] {name}: {location}")


def _summarize(tag: str, emails: Iterable[str]) -> str:
    values = sorted(set(emails))
    sample = ", ".join(values[:6])
    more = max(0, len(values) - 6)
    tail = f" …+{more}" if more else ""
    return f"{tag}: {len(values)} [{sample}{tail}]"


def _unpack_uploaded(result: Sequence[object] | set[str] | list[str]) -> tuple[set[str], set[str]]:
    if isinstance(result, (set, list, tuple)):
        if isinstance(result, tuple):
            if len(result) >= 2:
                allowed_raw = result[0]
                loose_raw = result[1]
            elif len(result) == 1:
                allowed_raw = result[0]
                loose_raw = set()
            else:  # pragma: no cover - defensive
                allowed_raw, loose_raw = set(), set()
        else:
            allowed_raw = result
            loose_raw = set()
    else:  # pragma: no cover - defensive
        allowed_raw, loose_raw = result, set()
    allowed = set(str(e).strip().lower() for e in allowed_raw)
    loose = set(str(e).strip().lower() for e in loose_raw)
    return allowed, loose


def _is_numeric_localpart(email: str) -> bool:
    local, _, _ = email.partition("@")
    return bool(local) and local.isdigit()


async def _from_url(url: str) -> tuple[set[str], set[str], list[str]]:
    import aiohttp

    async with aiohttp.ClientSession() as session:
        response = await async_extract_emails_from_url(url, session, "smoke")
    if not isinstance(response, tuple):  # pragma: no cover - defensive
        return set(), set(), []
    if len(response) >= 5:
        _, allowed_raw, foreign_raw, repairs_raw, *_ = response
    elif len(response) >= 4:
        _, allowed_raw, foreign_raw, repairs_raw = response
    elif len(response) >= 3:
        _, allowed_raw, foreign_raw = response
        repairs_raw = []
    else:  # pragma: no cover - defensive
        allowed_raw, foreign_raw, repairs_raw = set(), set(), []
    allowed = set(str(e).strip().lower() for e in allowed_raw)
    foreign = set(str(e).strip().lower() for e in foreign_raw)
    repairs = [str(r) for r in (repairs_raw or [])]
    return allowed, foreign, repairs


def _classify_foreign(candidates: Iterable[str]) -> set[str]:
    result = set()
    for email in candidates:
        if "@" not in email:
            continue
        domain = email.rsplit("@", 1)[1].lower()
        try:
            foreign = is_foreign_domain(domain)
        except Exception:  # pragma: no cover - defensive
            foreign = True
        if foreign:
            result.add(email)
    return result


def _fail(message: str) -> None:
    print(f"❌ {message}")
    raise SystemExit(1)


def _ok(message: str) -> None:
    print(f"✅ {message}")


def run_sanity(
    paths: Iterable[str],
    expects: Iterable[str],
    url: str | None,
    allow_numeric: bool,
    verbose: bool,
) -> None:
    if verbose:
        print_modules()

    all_allowed: set[str] = set()
    all_loose: set[str] = set()

    if url:
        allowed, foreign, _repairs = asyncio.run(_from_url(url))
        all_allowed.update(allowed)
        all_loose.update(foreign)

    for path in paths:
        abs_path = os.path.abspath(path)
        if not os.path.exists(abs_path):
            _fail(f"Файл не найден: {abs_path}")
        result = extract_from_uploaded_file(abs_path)
        allowed, loose = _unpack_uploaded(result)
        all_allowed.update(allowed)
        all_loose.update(loose)

    print(_summarize("allowed", all_allowed))
    print(_summarize("loose", all_loose))

    bad = {email for email in all_allowed if MIXED_BAD_RE.search(email)}
    if bad:
        _fail(f"Обнаружен мусор в 'allowed': {sorted(bad)[:8]}")
    _ok("Мусорных адресов 'poccия…/russia1…' не найдено")

    foreign = _classify_foreign(all_allowed | all_loose)
    print(_summarize("foreign", foreign))
    if foreign:
        _fail("Обнаружены иностранные домены в итоговом множестве (ожидалось только .ru/.com)")
    _ok("Иностранных доменов нет (разрешены только .ru/.com)")

    expects_lower = {e.lower() for e in expects if e}
    missing = sorted(email for email in expects_lower if email not in all_allowed)
    if missing:
        _fail(f"Не найдены ожидаемые адреса: {missing}")
    if expects_lower:
        _ok(f"Ожидаемые адреса присутствуют: {sorted(expects_lower)}")

    numeric = {email for email in all_allowed if _is_numeric_localpart(email)}
    if numeric and not allow_numeric:
        _fail(
            "Найдены чисто цифровые логины (по умолчанию не допускаются): "
            f"{sorted(numeric)[:8]}"
        )
    if numeric and allow_numeric:
        _ok(f"Цифровые логины разрешены ({len(numeric)})")

    print("\n— SMOKE OK —")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke sanity for email extractor")
    parser.add_argument("--pdf", nargs="*", default=[], help="Path(s) to PDF")
    parser.add_argument("--zip", nargs="*", default=[], help="Path(s) to ZIP with files")
    parser.add_argument("--txt", nargs="*", default=[], help="Path(s) to raw text/HTML")
    parser.add_argument("--url", default=None, help="URL to fetch and test")
    parser.add_argument("--expect", nargs="*", default=[], help="Expected emails to be present")
    parser.add_argument("--allow-numeric", action="store_true", help="Allow numeric-only local parts")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print imported modules")
    args = parser.parse_args()

    inputs = list(args.pdf) + list(args.zip) + list(args.txt)
    run_sanity(inputs, args.expect, args.url, args.allow_numeric, args.verbose)


if __name__ == "__main__":
    main()
