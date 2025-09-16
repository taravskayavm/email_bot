import os

__all__ = ["CONFUSABLES_NORMALIZE", "OBFUSCATION_ENABLE", "get_bool"]


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


CONFUSABLES_NORMALIZE = get_bool("CONFUSABLES_NORMALIZE", False)
OBFUSCATION_ENABLE = get_bool("OBFUSCATION_ENABLE", False)
