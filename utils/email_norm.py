from __future__ import annotations

import re

# Strict e-mail matcher with named groups for local and domain parts
_RX = re.compile(
    r"(?i)\b(?P<local>[-a-z0-9!#$%&'*+/=?^_`{|}~./]+)@(?P<domain>[a-z0-9.-]+\.[a-z]{2,})\b"
)


def sanitize_for_send(addr: str) -> str:
    """Return an address suitable for sending without altering the local part.

    * Keeps the local part *exactly* as in the source (no gmail tricks).
    * Normalises the domain: lower-case and IDNA-encodes if necessary.
    """

    s = (addr or "").strip()
    match = _RX.search(s)
    if not match:
        return ""
    local = match.group("local")
    domain = match.group("domain")
    try:
        domain_ascii = domain.encode("idna").decode("ascii").lower()
    except Exception:
        domain_ascii = domain.lower()
    return f"{local}@{domain_ascii}"


__all__ = ["sanitize_for_send"]
