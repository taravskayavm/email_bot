from utils.email_clean import extract_emails, dedupe_with_variants

def parse_emails_from_text(text: str) -> list[str]:
    emails = extract_emails(text)
    return dedupe_with_variants(emails)
