from utils.email_clean import parse_emails_unified, dedupe_with_variants

def parse_emails_from_text(text: str) -> list[str]:
    emails = parse_emails_unified(text)
    return dedupe_with_variants(emails)
