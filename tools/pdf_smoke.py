"""Batch PDF smoke-test helper."""

from __future__ import annotations

import json
from pathlib import Path

from emailbot.extraction_pdf import extract_from_pdf_stream


ROOT = Path(__file__).resolve().parents[1]
PDF_DIR = ROOT / "tests/fixtures/pdfs"
OUT = ROOT / "var/pdf_smoke_report.jsonl"


def run_one(pdf_path: Path) -> dict:
    data = pdf_path.read_bytes()
    hits, stats = extract_from_pdf_stream(data, source_ref=str(pdf_path))
    emails = sorted({hit.email for hit in hits})
    suspects = stats.get("emails_suspects") or stats.get("suspects") or []
    return {
        "file": str(pdf_path),
        "emails": emails,
        "emails_count": len(emails),
        "suspects_count": len(suspects),
        "stats": {k: v for k, v in stats.items() if isinstance(v, (int, str))},
    }


def main() -> None:
    if not PDF_DIR.exists():
        print("[skip] no tests/fixtures/pdfs directory")
        return
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as handle:
        for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
            try:
                row = run_one(pdf_path)
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                print(
                    f"[ok] {pdf_path.name}: {row['emails_count']} emails "
                    f"(suspects {row['suspects_count']})"
                )
            except Exception as exc:  # pragma: no cover - diagnostic output
                print(f"[warn] {pdf_path}: {exc}")


if __name__ == "__main__":
    main()
