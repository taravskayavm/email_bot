import pytest


@pytest.mark.parametrize(
    ("text", "expected_email", "expected_context", "is_role"),
    [
        ("Contact us at info@example.com", "info@example.com", "contacts", True),
        ("Link: mailto:support@example.com", "support@example.com", "mailto", True),
        (
            "Corresponding author: cor@example.com",
            "cor@example.com",
            "pdf_corresponding_author",
            False,
        ),
        ("Lead author is lead@example.com", "lead@example.com", "author_block", False),
        ("Footer notes: copy@example.com", "copy@example.com", "footer", False),
        ("Write hi@example.com for details", "hi@example.com", "unknown", False),
    ],
)
def test_extract_emails_pipeline_source_context(
    monkeypatch, text, expected_email, expected_context, is_role
):
    captured_meta = {}

    from pipelines import extract_emails as pipeline

    original_parse = pipeline.parse_emails_unified

    def capture_parse(raw_text: str, return_meta: bool = False):
        cleaned, meta = original_parse(raw_text, return_meta=True)
        captured_meta["meta"] = meta
        if return_meta:
            return cleaned, meta
        return cleaned

    monkeypatch.setattr(pipeline, "parse_emails_unified", capture_parse)

    emails, stats = pipeline.extract_emails_pipeline(text)

    if is_role and pipeline.PERSONAL_ONLY:
        assert expected_email not in emails
        items = captured_meta["meta"]["items"]
        assert any(
            (item.get("sanitized") or item.get("normalized")) == expected_email
            and item.get("reason") == "role-like-prefix"
            for item in items
        )
    else:
        assert emails == [expected_email]
        assert stats["contexts_tagged"] >= 1

    contexts = captured_meta["meta"]["source_context"]
    assert contexts.get(expected_email) == expected_context
    assert stats["contexts_tagged"] >= 1
