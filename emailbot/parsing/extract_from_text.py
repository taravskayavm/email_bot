from .email_patterns import extract_emails
from .validators import filter_by_mx


def emails_from_text(text: str) -> set[str]:
    return filter_by_mx(extract_emails(text))
