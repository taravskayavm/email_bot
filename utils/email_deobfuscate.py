"""Robust helpers to deobfuscate e-mail addresses in text."""

from __future__ import annotations

import os
from typing import Iterable, Callable, Set, Tuple

try:  # pragma: no cover - optional dependency
    import regex as _regex  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - environments without ``regex``
    _regex = None  # type: ignore[assignment]

import re as _re  # type: ignore[no-redef]

__all__ = ["deobfuscate_text"]


# Настройки, управляемые через переменные окружения (.env)
_DEOBF_MAX_CHARS = int(os.getenv("DEOBF_MAX_CHARS", "2000000"))
_DEOBF_CHUNK_SIZE = int(os.getenv("DEOBF_CHUNK_SIZE", "200000"))
_DEOBF_OVERLAP = int(os.getenv("DEOBF_OVERLAP", "64"))
_DEOBF_TIMEOUT_MS = int(os.getenv("DEOBF_TIMEOUT_MS", "50"))
_DEOBF_MAX_REPLACES = int(os.getenv("DEOBF_MAX_REPLACES", "5000"))

_DEOBF_TIMEOUT_SEC = max(_DEOBF_TIMEOUT_MS, 0) / 1000.0
_COUNT_LIMIT = max(_DEOBF_MAX_REPLACES, 0)

_regex_mod = _regex or _re
_TIMEOUT_ERRORS: Tuple[type[BaseException], ...]
if _regex is not None and hasattr(_regex, "TimeoutError"):
    _TIMEOUT_ERRORS = (_regex.TimeoutError,)  # type: ignore[attr-defined]
else:
    _TIMEOUT_ERRORS = tuple()

try:  # pragma: no cover - optional dependency
    from emailbot.progress_watchdog import heartbeat_now as _heartbeat_now
except Exception:  # pragma: no cover - avoid circular imports during startup
    _heartbeat_now = None  # type: ignore[assignment]


def _heartbeat() -> None:
    """Emit watchdog heartbeat if the helper is available."""

    global _heartbeat_now  # type: ignore[assignment]
    if _heartbeat_now is None:
        try:
            from emailbot.progress_watchdog import heartbeat_now as _hb
        except Exception:
            return
        _heartbeat_now = _hb  # type: ignore[assignment]
    try:
        _heartbeat_now()  # type: ignore[misc]
    except Exception:  # pragma: no cover - heartbeat best effort
        pass


def _iter_chunks(data: str, size: int, overlap: int) -> Iterable[str]:
    """Yield ``data`` in chunks with the requested ``overlap``."""

    if size <= 0 or len(data) <= size:
        yield data
        return

    n = len(data)
    step = max(size - max(overlap, 0), 1)
    start = 0
    while start < n:
        end = min(start + size, n)
        yield data[start:end]
        if end == n:
            break
        start += step


def _subn_kwargs() -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if _COUNT_LIMIT > 0:
        kwargs["count"] = _COUNT_LIMIT
    if _regex is not None and _DEOBF_TIMEOUT_SEC > 0.0:
        kwargs["timeout"] = _DEOBF_TIMEOUT_SEC
    return kwargs


def _safe_subn(
    pattern,
    text: str,
    repl: Callable[[object], str] | str,
    *,
    rule_name: str,
    rules: Set[str],
) -> Tuple[str, int]:
    """Run ``pattern.subn`` with limits and guard against catastrophic cases."""

    kwargs = _subn_kwargs()
    try:
        if callable(repl):
            def _wrapped(match):
                rules.add(rule_name)
                return repl(match)

            return pattern.subn(_wrapped, text, **kwargs)
        new_text, count = pattern.subn(repl, text, **kwargs)
        if count:
            rules.add(rule_name)
        return new_text, count
    except _TIMEOUT_ERRORS:
        return text, 0


def _word_pattern(base: str) -> str:
    """Allow obfuscated words like "s o b a k a" or "d-o-t"."""

    letters = list(base)
    if not letters:
        return ""
    rest = [rf"\W?{_regex_mod.escape(ch)}" for ch in letters[1:]]
    return rf"{_regex_mod.escape(letters[0])}\s*" + r"\s*".join(rest)


_LOCAL_FRAGMENT = r"[\w.%+\-]{1,64}"
_DOMAIN_LABEL = r"[A-Za-z0-9-]{1,63}"

# Разделитель между local-part и обфусцированным «at»: хотя бы один не-алфанумерик.
_AT_SEP = r"\W+"

_PAT_ATS = [
    _regex_mod.compile(
        rf"(?P<L>{_LOCAL_FRAGMENT}){_AT_SEP}(?:{_word_pattern('собака')}|{_word_pattern('at')}){_AT_SEP}(?P<R>{_DOMAIN_LABEL}(?:\.{_DOMAIN_LABEL})*)",
        _regex_mod.IGNORECASE,
    )
]

_PAT_DOTS = [
    _regex_mod.compile(
        rf"(?P<L>{_DOMAIN_LABEL})\s*[\(\[\{{]?\s*(?:{_word_pattern('точка')}|{_word_pattern('dot')})\s*[\)\]\}}]?\s*(?P<R>{_DOMAIN_LABEL})",
        _regex_mod.IGNORECASE,
    )
]

_PAT_SPACED_LETTERS = [
    _regex_mod.compile(r"\b(?:[A-Za-z0-9]\s+){1,}[A-Za-z0-9]\b")
]

_HYPHEN_RE = _regex_mod.compile(r"(?<=\w)\s*-\s*(?=\w)")


def _deobfuscate_chunk(text: str, rules: Set[str]) -> str:
    """Apply deobfuscation rules to ``text`` without chunk management."""

    if not text:
        return text

    current = text

    def _apply(patterns, repl_func: Callable[[object], str], rule_name: str) -> bool:
        nonlocal current
        changed = False
        for pat in patterns:
            new_current, count = _safe_subn(pat, current, repl_func, rule_name=rule_name, rules=rules)
            if count:
                current = new_current
                changed = True
        return changed

    while True:
        changed = False
        if _apply(
            _PAT_DOTS,
            lambda m: f"{m.group('L')}.{m.group('R')}",
            "dot",
        ):
            changed = True
        if _apply(
            _PAT_ATS,
            lambda m: f"{m.group('L')}@{m.group('R')}",
            "at",
        ):
            changed = True
        if _apply(
            _PAT_SPACED_LETTERS,
            lambda m: m.group(0).replace(" ", ""),
            "spaced",
        ):
            changed = True
        new_current, hyphen_count = _safe_subn(
            _HYPHEN_RE,
            current,
            "-",
            rule_name="hyphen",
            rules=rules,
        )
        if hyphen_count:
            current = new_current
            changed = True
        if not changed:
            break
    return current


def _set_last_rules(rules: Iterable[str]) -> None:
    try:
        deobfuscate_text.last_rules = list(rules)  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - defensive
        pass


def deobfuscate_text(text: str) -> str:
    """Return text with simple e-mail obfuscations normalised."""

    if not text:
        _set_last_rules([])
        return text

    if _DEOBF_MAX_CHARS > 0 and len(text) > _DEOBF_MAX_CHARS:
        _set_last_rules([])
        return text

    rules: Set[str] = set()

    if _DEOBF_CHUNK_SIZE <= 0 or len(text) <= _DEOBF_CHUNK_SIZE:
        _heartbeat()
        result = _deobfuscate_chunk(text, rules)
        _set_last_rules(sorted(rules))
        return result

    chunk_size = max(_DEOBF_CHUNK_SIZE, 1)
    if chunk_size == 1:
        overlap = 0
    else:
        overlap = max(min(_DEOBF_OVERLAP, chunk_size - 1), 0)
    step = max(chunk_size - overlap, 1)

    parts: list[str] = []
    carry = ""
    start = 0
    first_chunk = True
    text_len = len(text)

    while start < text_len:
        chunk = text[start : start + chunk_size]
        if not chunk:
            break
        _heartbeat()
        combined = carry + chunk
        processed = _deobfuscate_chunk(combined, rules)

        has_more = start + chunk_size < text_len
        safe_limit = len(processed) - overlap if has_more else len(processed)
        if safe_limit < 0:
            safe_limit = 0

        prefix_limit = min(len(carry), safe_limit)
        if prefix_limit:
            parts.append(processed[:prefix_limit])

        emit_start = prefix_limit
        if not first_chunk:
            skip = min(overlap, len(chunk))
            if skip and emit_start < safe_limit:
                emit_start = min(emit_start + skip, safe_limit)

        if emit_start < safe_limit:
            parts.append(processed[emit_start:safe_limit])

        carry = processed[safe_limit:]
        first_chunk = False
        start += step

    if carry:
        parts.append(carry)

    result = "".join(parts)
    _set_last_rules(sorted(rules))
    return result


try:  # pragma: no cover - default value for attribute
    deobfuscate_text.last_rules = []  # type: ignore[attr-defined]
except Exception:
    pass
