def test_module_imports():
    # Раньше f-строка с {dot} ломала импорт. Теперь импорт должен проходить.
    import importlib
    m = importlib.import_module("emailbot.extraction_url")
    assert hasattr(m, "_OBFUSCATED_RE")


def test_obfuscated_allows_curly_dot_and_mailto():
    from emailbot.extraction_url import extract_obfuscated_hits

    html = """
      <p>name {at} example {dot} com</p>
      <a href="mailto:info@пример.рф?subject=x">mail us</a>
    """
    hits = extract_obfuscated_hits(html, "dummy")
    emails = {h.email for h in hits}
    assert "name@example.com" in emails
    assert any(x.endswith("@xn--e1afmkfd.xn--p1ai") for x in emails)
