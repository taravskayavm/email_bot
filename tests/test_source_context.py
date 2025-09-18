import pytest


@pytest.mark.parametrize(
    ("text", "expected_email", "expected_context"),
    [
        ("Contact us at info@example.com", "info@example.com", "contacts"),
        ("Link: mailto:support@example.com", "support@example.com", "mailto"),
        (
            "Corresponding author: cor@example.com",
            "cor@example.com",
            "pdf_corresponding_author",
        ),
        ("Lead author is lead@example.com", "lead@example.com", "author_block"),
        ("Footer notes: copy@example.com", "copy@example.com", "footer"),
        ("Write hi@example.com for details", "hi@example.com", "unknown"),
    ],
)
def test_extract_emails_pipeline_source_context(monkeypatch, text, expected_email, expected_context):
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

    assert emails == [expected_email]
    assert stats["contexts_tagged"] == 1

    contexts = captured_meta["meta"]["source_context"]
    assert contexts.get(expected_email) == expected_context
