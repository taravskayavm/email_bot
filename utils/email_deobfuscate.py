import re

__all__ = ["deobfuscate_text"]

_LOCAL_FRAGMENT = r"[A-Za-z0-9._%+-]{1,64}"
_DOMAIN_LABEL = r"[A-Za-z0-9-]{1,63}"
_DOMAIN_FRAGMENT = rf"(?:{_DOMAIN_LABEL}\.)+[A-Za-z]{{2,24}}"
_LETTER_CLASS = "A-Za-zА-Яа-яЁё"
_GAP = rf"(?:[^{_LETTER_CLASS}])?"


def _spaced(word: str) -> str:
    if not word:
        return ""
    parts = [re.escape(word[0])]
    for ch in word[1:]:
        parts.append(_GAP)
        parts.append(re.escape(ch))
    return "".join(parts)


_AT_WORD = rf"(?:{_spaced('at')}|{_spaced('собака')})"
_DOT_WORD = rf"(?:{_spaced('dot')}|{_spaced('точка')})"
_AT_WRAPPED = rf"[\(\[\{{]?\s*{_AT_WORD}\s*[\)\]\}}]?"
_DOT_WRAPPED = rf"[\(\[\{{]?\s*{_DOT_WORD}\s*[\)\]\}}]?"

_DOT_PATTERN = re.compile(
    rf"(?<![A-Za-z0-9-])(?P<left>{_DOMAIN_LABEL})\s*{_DOT_WRAPPED}\s*(?P<right>{_DOMAIN_LABEL})(?![A-Za-z0-9-])",
    re.IGNORECASE,
)

_AT_PATTERN = re.compile(
    rf"(?<![A-Za-z0-9._%+-])(?P<local>{_LOCAL_FRAGMENT})\s*{_AT_WRAPPED}\s*(?P<domain>{_DOMAIN_FRAGMENT})(?![A-Za-z0-9-])",
    re.IGNORECASE,
)


def deobfuscate_text(text: str) -> str:
    """Return text with simple e-mail obfuscations normalised."""

    if not text:
        deobfuscate_text.last_rules = []
        return text

    rules: set[str] = set()
    current = text

    def _dot_repl(match: re.Match) -> str:
        rules.add("dot")
        return f"{match.group('left')}.{match.group('right')}"

    def _at_repl(match: re.Match) -> str:
        rules.add("at")
        return f"{match.group('local')}@{match.group('domain')}"

    while True:
        current, count = _DOT_PATTERN.subn(_dot_repl, current)
        if count == 0:
            break

    current, _ = _AT_PATTERN.subn(_at_repl, current)

    deobfuscate_text.last_rules = sorted(rules)
    return current


# Attach default attribute for introspection
try:
    deobfuscate_text.last_rules = []  # type: ignore[attr-defined]
except Exception:
    pass
