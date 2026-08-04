"""Persistent deny-list for recipient domains that reject external mail."""

from __future__ import annotations

import os
import re
from pathlib import Path
from threading import RLock
from typing import Iterable
from urllib.parse import urlsplit

import idna

DEFAULT_BLOCKED_DOMAINS = frozenset(
    {
        "163.com",
        "cardiotomsk.ru",
        "emcmos.ru",
        "ion.com",
        "ngs.ru",
        "npcmr.ru",
        "qq.com",
    }
)

_LOCK = RLock()
_CACHE: set[str] = set()
_MTIME_NS: int | None = None


def _default_path() -> Path:
    base = Path(os.getenv("EMAILBOT_DATA_DIR") or Path.cwd()).expanduser()
    return base.resolve() / "blocked_domains.txt"


_BLOCKED_DOMAINS_PATH = _default_path()


def blocked_domains_path() -> Path:
    return _BLOCKED_DOMAINS_PATH


def normalize_domain(value: str) -> str:
    """Normalize a domain, URL, ``@domain`` or e-mail input."""

    raw = (value or "").strip().lower().rstrip(".,;:")
    if not raw:
        return ""
    if "@" in raw:
        raw = raw.rsplit("@", 1)[1]
    raw = raw.removeprefix("@")
    if "://" in raw:
        raw = urlsplit(raw).hostname or ""
    else:
        parsed = urlsplit("//" + raw)
        raw = parsed.hostname or raw
    raw = raw.strip().strip(".")
    if raw.startswith("*."):
        raw = raw[2:]
    if not raw or len(raw) > 253 or "." not in raw:
        return ""
    try:
        ascii_domain = idna.encode(raw, uts46=True).decode("ascii").lower()
    except Exception:
        return ""
    labels = ascii_domain.split(".")
    if any(
        not label
        or len(label) > 63
        or label.startswith("-")
        or label.endswith("-")
        or re.fullmatch(r"[a-z0-9-]+", label) is None
        for label in labels
    ):
        return ""
    if len(labels[-1]) < 2 or labels[-1].isdigit():
        return ""
    return ascii_domain


def parse_domains(text: str) -> tuple[list[str], list[str]]:
    """Parse a user-entered domain list into valid and rejected tokens."""

    valid: list[str] = []
    rejected: list[str] = []
    seen: set[str] = set()
    for token in re.split(r"[\s,;]+", text or ""):
        token = token.strip()
        if not token:
            continue
        domain = normalize_domain(token)
        if not domain:
            rejected.append(token)
            continue
        if domain not in seen:
            seen.add(domain)
            valid.append(domain)
    return valid, rejected


def _write_locked(domains: Iterable[str]) -> None:
    _BLOCKED_DOMAINS_PATH.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(set(domains))
    data = "\n".join(ordered) + ("\n" if ordered else "")
    _BLOCKED_DOMAINS_PATH.write_text(data, encoding="utf-8")


def _read_locked() -> set[str]:
    if not _BLOCKED_DOMAINS_PATH.exists():
        _write_locked(DEFAULT_BLOCKED_DOMAINS)
    values: set[str] = set()
    for line in _BLOCKED_DOMAINS_PATH.read_text(encoding="utf-8").splitlines():
        domain = normalize_domain(line)
        if domain:
            values.add(domain)
    return values


def _ensure_loaded_locked() -> None:
    global _CACHE, _MTIME_NS
    try:
        mtime_ns = _BLOCKED_DOMAINS_PATH.stat().st_mtime_ns
    except FileNotFoundError:
        mtime_ns = None
    if mtime_ns == _MTIME_NS and mtime_ns is not None:
        return
    _CACHE = _read_locked()
    try:
        _MTIME_NS = _BLOCKED_DOMAINS_PATH.stat().st_mtime_ns
    except FileNotFoundError:
        _MTIME_NS = None


def load_blocked_domains() -> set[str]:
    with _LOCK:
        _ensure_loaded_locked()
        return set(_CACHE)


def add_blocked_domains(domains: Iterable[str]) -> int:
    global _CACHE, _MTIME_NS
    normalized = {domain for item in domains if (domain := normalize_domain(item))}
    if not normalized:
        return 0
    with _LOCK:
        _ensure_loaded_locked()
        before = len(_CACHE)
        updated = _CACHE | normalized
        if len(updated) == before:
            return 0
        _write_locked(updated)
        _CACHE = updated
        _MTIME_NS = _BLOCKED_DOMAINS_PATH.stat().st_mtime_ns
        return len(updated) - before


def add_blocked_domain(domain: str) -> bool:
    return add_blocked_domains([domain]) > 0


def is_blocked_domain(domain: str) -> bool:
    normalized = normalize_domain(domain)
    if not normalized:
        return False
    blocked = load_blocked_domains()
    return any(
        normalized == item or normalized.endswith("." + item)
        for item in blocked
    )


def is_blocked_email_domain(email: str) -> bool:
    if "@" not in (email or ""):
        return False
    return is_blocked_domain(email.rsplit("@", 1)[1])


def init_blocked_domains(path: str | Path | None = None) -> None:
    """Initialize the store, optionally overriding its path for tests."""

    global _BLOCKED_DOMAINS_PATH, _CACHE, _MTIME_NS
    with _LOCK:
        if path is not None:
            _BLOCKED_DOMAINS_PATH = Path(path)
        _CACHE = set()
        _MTIME_NS = None
        _ensure_loaded_locked()


__all__ = [
    "DEFAULT_BLOCKED_DOMAINS",
    "add_blocked_domain",
    "add_blocked_domains",
    "blocked_domains_path",
    "init_blocked_domains",
    "is_blocked_domain",
    "is_blocked_email_domain",
    "load_blocked_domains",
    "normalize_domain",
    "parse_domains",
]
