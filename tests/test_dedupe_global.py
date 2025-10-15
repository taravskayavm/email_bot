from emailbot.dedupe_global import dedupe_across_sources


def test_cross_source_dedupe():
    hits = [
        {"email": "A@EXAMPLE.com", "source": "file1.pdf"},
        {"email": "a@example.com", "source": "file2.docx"},
    ]
    unique, duplicates = dedupe_across_sources(hits)

    assert len(unique) == 1
    assert len(next(iter(duplicates.values()))) == 1
