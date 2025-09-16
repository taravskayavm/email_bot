try:  # pragma: no cover - optional dependency
    import regex as re  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - fallback for environments without "regex"
    import re  # type: ignore[no-redef]

__all__ = ["deobfuscate_text"]


def _word_pattern(base: str) -> str:
    """Allow obfuscated words like "s o b a k a" or "d-o-t"."""

    letters = list(base)
    if not letters:
        return ""
    rest = [rf"\W?{re.escape(ch)}" for ch in letters[1:]]
    return rf"{re.escape(letters[0])}\s*" + "\s*".join(rest)


_LOCAL_FRAGMENT = r"[\w.%+\-]{1,64}"
_DOMAIN_LABEL = r"[A-Za-z0-9-]{1,63}"

_PAT_ATS = [
    re.compile(
        rf"(?P<L>{_LOCAL_FRAGMENT})\s*[\(\[\{{]?\s*(?:{_word_pattern('собака')}|{_word_pattern('at')})\s*[\)\]\}}]?\s*(?P<R>{_DOMAIN_LABEL}(?:\.{_DOMAIN_LABEL})*)",
        re.IGNORECASE,
    )
]

_PAT_DOTS = [
    re.compile(
        rf"(?P<L>{_DOMAIN_LABEL})\s*[\(\[\{{]?\s*(?:{_word_pattern('точка')}|{_word_pattern('dot')})\s*[\)\]\}}]?\s*(?P<R>{_DOMAIN_LABEL})",
        re.IGNORECASE,
    )
]


def deobfuscate_text(text: str) -> str:
    """Return text with simple e-mail obfuscations normalised."""

    if not text:
        try:
            deobfuscate_text.last_rules = []  # type: ignore[attr-defined]
        except Exception:
            pass
        return text

    rules: set[str] = set()
    current = text

    def _sub_all(patterns: list[re.Pattern], repl_func, rule_name: str) -> bool:
        nonlocal current
        changed = False
        for pat in patterns:
            new_current, count = pat.subn(
                lambda m, rf=repl_func, rn=rule_name: (
                    rules.add(rn),
                    rf(m),
                )[1],
                current,
            )
            if count:
                current = new_current
                changed = True
        return changed

    while True:
        changed = False
        if _sub_all(
            _PAT_DOTS,
            lambda m: f"{m.group('L')}.{m.group('R')}",
            "dot",
        ):
            changed = True
        if _sub_all(
            _PAT_ATS,
            lambda m: f"{m.group('L')}@{m.group('R')}",
            "at",
        ):
            changed = True
        if not changed:
            break

    try:
        deobfuscate_text.last_rules = sorted(rules)  # type: ignore[attr-defined]
    except Exception:
        pass
    return current


try:  # pragma: no cover - default value for attribute
    deobfuscate_text.last_rules = []  # type: ignore[attr-defined]
except Exception:
    pass
