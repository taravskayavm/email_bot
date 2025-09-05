from utils.email_clean import sanitize_email, dedupe_with_variants, _strip_leading_footnote


def ingest(all_extracted_emails: list[str]) -> tuple[list[str], str]:
    emails = dedupe_with_variants([sanitize_email(e) for e in all_extracted_emails])

    before = set(all_extracted_emails)              # до sanitize+dedupe
    after = set(emails)                             # после sanitize+dedupe

    def _key(e: str) -> str:
        local, domain = e.split('@', 1)
        return f"{_strip_leading_footnote(local)}@{domain}"

    before_keys = {_key(e) for e in before}
    lost_as_variants = len(before) - len(before_keys)
    footnote_removed = max(0, lost_as_variants)

    found = len(before)
    stats = (
        f"✅ Анализ завершён.\n"
        f"Найдено адресов: {found}\n"
        f"Уникальных (после очистки): {len(emails)}\n"
        f"Возможные сносочные дубликаты удалены: {footnote_removed}"
    )

    return emails, stats
