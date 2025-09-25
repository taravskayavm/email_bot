#!/usr/bin/env python3
"""Utility CLI for extraction smoke tests and diagnostics."""

from __future__ import annotations

import argparse
import imaplib
import os
import pathlib
from email.message import EmailMessage

from emailbot import messaging
from emailbot.extraction import extract_from_pdf, smart_extract_emails, strip_html


def _run_extractor() -> None:
    base = pathlib.Path("tests/fixtures/gold")
    for path in sorted(base.iterdir()):
        if path.suffix == ".pdf":
            hits, stats = extract_from_pdf(str(path))
            count = len(hits)
            quarantined = stats.get("quarantined", 0)
        else:
            text = strip_html(path.read_text(encoding="utf-8"))
            stats: dict = {}
            emails = smart_extract_emails(text, stats)
            count = len(emails)
            quarantined = stats.get("quarantined", 0)
        print(f"{path.name}: {count} ok, {quarantined} quarantined")


def _check_sent_append() -> None:
    addr = os.getenv("EMAIL_ADDRESS") or messaging.EMAIL_ADDRESS
    pwd = os.getenv("EMAIL_PASSWORD") or messaging.EMAIL_PASSWORD
    if not addr or not pwd:
        raise SystemExit("EMAIL_ADDRESS/EMAIL_PASSWORD not configured")
    imap = imaplib.IMAP4_SSL("imap.mail.ru")
    imap.login(addr, pwd)
    folder = messaging.get_preferred_sent_folder(imap)
    msg = EmailMessage()
    msg["From"] = addr
    msg["To"] = addr
    msg.set_content("")
    status, _ = imap.append(f'"{folder}"', "", None, msg.as_bytes())
    imap.logout()
    if status != "OK":
        raise RuntimeError("APPEND failed")
    print(f"APPEND to {folder}: OK")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check-sent-append",
        action="store_true",
        help="verify that APPEND to detected Sent folder succeeds",
    )
    parser.add_argument(
        "--scan-bounce",
        action="store_true",
        help="scan INBOX for bounces (IMAP/POP3 per .env)",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="print summary report for last N days (default 180)",
    )
    parser.add_argument(
        "--report-days",
        type=int,
        default=int(os.getenv("REPORT_DAYS", "180")),
        help="days back for the summary report",
    )
    args = parser.parse_args()

    if args.scan_bounce:
        from utils.bounce import scan_bounces

        count = scan_bounces()
        print(f"Bounces logged: {count}")
    elif args.report:
        from utils.send_stats import print_summary_report

        print_summary_report(days=args.report_days)
    elif args.check_sent_append:
        _check_sent_append()
    else:
        _run_extractor()


if __name__ == "__main__":
    main()
