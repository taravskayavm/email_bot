"""Offline registry of TLDs used for validation."""

from __future__ import annotations

from typing import Set

# Hardcoded list of known TLDs (uppercase, IDNA/ASCII).
# This list includes generic TLDs, common ccTLDs and domestic ones.
KNOWN_TLDS: Set[str] = {
    "COM", "ORG", "NET", "EDU", "GOV", "MIL", "INT", "INFO", "BIZ", "NAME",
    "PRO", "AERO", "COOP", "MUSEUM", "TRAVEL", "MOBI", "ONLINE", "SITE",
    "AGENCY", "APP", "DEV", "IO", "AI", "CLUB", "XYZ", "TOP", "STORE",
    "TECH",
    # Domestic and nearby ccTLDs
    "RU", "SU", "BY", "KZ", "UA", "UZ", "KG", "AM", "AZ", "GE", "XN--P1AI",
    "XN--80ASEHDB",  # .ОНЛАЙН
    # Common ccTLDs
    "US", "UK", "CA", "DE", "FR", "IT", "PL", "CZ", "SK", "CH", "SE", "NO",
    "FI", "ES", "PT", "NL", "BE", "TR", "CN", "JP", "KR", "LT", "LV", "EE",
    "IN", "BR", "AR", "AU", "NZ", "AT", "DK", "GR", "HU", "RO", "RS", "BG",
    "MD", "IL", "IE", "HK", "SG", "MY", "ID", "TH", "VN", "PK", "AE", "QA",
    "SA", "EG", "MA", "TN", "AL", "MK", "BA", "HR", "SI", "ME", "IS", "LI",
    "ZA", "NG", "KE",
}


def tld_of(domain: str) -> str | None:
    """Return last label of ``domain`` as ASCII uppercase TLD."""

    if not domain:
        return None
    try:
        ascii_domain = domain.encode("idna").decode("ascii")
    except Exception:
        return None
    if "." not in ascii_domain:
        return None
    tld = ascii_domain.rsplit(".", 1)[-1]
    if not tld:
        return None
    return tld.upper()


def is_known_tld(tld: str) -> bool:
    """Return True if ``tld`` is in the known TLD registry."""

    return tld.upper() in KNOWN_TLDS
