"""Role vs personal e-mail heuristics."""

from __future__ import annotations

import re
from typing import Dict, Iterable

# Keywords frequently used for shared/departmental mailboxes.
#
# The list combines English and Russian cues that previously lived in
# ``utils.email_clean`` and a couple of extra synonyms that show up in the
# gold fixtures (editorial, journal, press, reception, etc.).
ROLE_KEYWORDS: frozenset[str] = frozenset(
    {
        "info",
        "kontakt",
        "contact",
        "service",
        "support",
        "help",
        "sales",
        "office",
        "press",
        "pressa",
        "editor",
        "editors",
        "editorial",
        "journals",
        "journal",
        "admissions",
        "career",
        "hr",
        "department",
        "dean",
        "reception",
        "priem",
        "otdel",
        "kafedra",
        "dekanat",
        "spravka",
        "redak",
        "rekto",
        "uchsec",
        "magistr",
        "bakalavr",
        "aspirant",
        "nauka",
        "kantsel",
        "public",
        "ojs",
        "mailer",
        "mail",
        "postmaster",
        "webmaster",
    }
)

# Patterns that should always be treated as role accounts even when tokenisation
# does not catch them (e.g. ``do-not-reply`` → tokens ``{"do", "not", "reply"}``).
ROLE_LOCAL_RE = re.compile(
    r"^(?:"
    r"no[-_.]?reply|"
    r"do[-_.]?not[-_.]?reply|"
    r"postmaster|"
    r"webmaster|"
    r"mailer(?:[-_.]?daemon)?|"
    r"mailbot|"
    r"bounce"
    r")$",
    re.IGNORECASE,
)

PERSONAL_HINT_RE = re.compile(
    r"author|corresponding\s+author|автор|корресп(?:онденц)?и?рующ",
    re.IGNORECASE,
)

ROLE_CONTEXT_RE = re.compile(
    r"кафедр|редакц|при[её]мн|отдел|служб|департамент|department|office|"
    r"support|centre|center|комитет|институт|faculty|факультет|press",
    re.IGNORECASE,
)

FIO_RE = re.compile(
    r"^[a-zа-яё]+(?:[._-][a-zа-яё]+){0,2}(?:[._-][a-zа-яё]{1,3})?$",
    re.IGNORECASE,
)


def _tokenise(parts: str) -> set[str]:
    return {t.strip("-_.") for t in re.split(r"[._+\-]", parts) if t}


def _merge_reasons(reasons: Iterable[str]) -> str:
    unique: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if not reason:
            continue
        if reason not in seen:
            seen.add(reason)
            unique.append(reason)
    return ",".join(unique) if unique else "baseline"


def classify_email_role(
    local: str, domain: str, context_text: str = ""
) -> Dict[str, object]:
    """Return heuristics-based role classification.

    The output follows the ``{"class", "score", "reason"}`` convention used by
    the extraction pipeline. The score is normalised to ``[0.0, 1.0]`` with
    ``0.5`` representing an undecided baseline. Results are:

    ``role``
        Highly confident that the address points to a shared mailbox.
    ``personal``
        Looks like a personal mailbox (FIO pattern or explicit hint).
    ``unknown``
        Not enough evidence either way; callers may keep such addresses.
    """

    local = (local or "").strip()
    domain = (domain or "").strip()

    if not local or "@" in local:
        return {"class": "unknown", "score": 0.5, "reason": "invalid-local"}

    local_lower = local.lower()
    reason: list[str] = []

    if ROLE_LOCAL_RE.match(local_lower):
        return {"class": "role", "score": 0.0, "reason": "role-local"}

    tokens = _tokenise(local_lower)
    if tokens & ROLE_KEYWORDS:
        return {"class": "role", "score": 0.0, "reason": "role-local"}

    score = 0.5

    domain_tokens = _tokenise(domain.lower()) if domain else set()
    if domain_tokens & ROLE_KEYWORDS:
        score -= 0.15
        reason.append("role-domain")

    if FIO_RE.match(local):
        score += 0.25
        reason.append("fio-like")

    ctx = (context_text or "").lower()
    if PERSONAL_HINT_RE.search(ctx):
        score += 0.15
        reason.append("author-context")

    if ROLE_CONTEXT_RE.search(ctx):
        score -= 0.2
        reason.append("dept-context")

    score = max(0.0, min(1.0, score))
    if score <= 0.35:
        cls = "role"
    elif score >= 0.65:
        cls = "personal"
    else:
        cls = "unknown"

    return {"class": cls, "score": round(score, 3), "reason": _merge_reasons(reason)}


__all__ = ["classify_email_role"]

