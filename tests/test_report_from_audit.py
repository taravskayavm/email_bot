import json

from emailbot.bot_handlers import _summarize_from_audit
from emailbot.messaging import OUTCOME


def test_report_metrics_from_audit(tmp_path):
    audit = tmp_path / "bulk_audit_test.jsonl"
    rows = [
        {"email": "a@x", "outcome": OUTCOME["sent"]},
        {"email": "b@x", "outcome": OUTCOME["blocked"]},
        {"email": "c@x", "outcome": OUTCOME["cooldown"]},
        {"email": "d@x", "outcome": OUTCOME["undeliverable"]},
        {"email": "e@x", "outcome": OUTCOME["error"]},
        {"email": "f@x", "outcome": OUTCOME["unchanged"]},
        {"email": "g@x", "outcome": "SENT"},
        {"email": "h@x", "outcome": "unknown"},
    ]
    with audit.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")

    metrics = _summarize_from_audit(str(audit))
    assert metrics["total"] == len(rows)
    assert metrics["sent"] == 2
    assert metrics["blocked"] == 1
    assert metrics["cooldown"] == 1
    assert metrics["undeliverable_only"] == 1
    assert metrics["errors"] == 2
    assert metrics["unchanged"] == 1
