from bs4 import BeautifulSoup

from .email_patterns import extract_emails
from .validators import filter_by_mx


def emails_from_html(html: str) -> set[str]:
    if not html:
        return set()
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    found = set(extract_emails(text))
    # mailto:
    for a in soup.find_all("a", href=True):
        href = a["href"] or ""
        if href.lower().startswith("mailto:"):
            found |= extract_emails(href[7:])
    return filter_by_mx(found)
