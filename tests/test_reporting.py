from emailbot.reporting import build_mass_report_text


def test_build_mass_report_text_ignores_blocked():
    sent_ok = ["a@example.com", "b@example.com"]
    skipped = ["c@example.com"]
    blocked_foreign = ["foreign@example.de"]
    blocked_invalid = ["invalid@example.com"]

    text = build_mass_report_text(sent_ok, skipped, blocked_foreign, blocked_invalid)

    assert "В блок" not in text
    assert "иностранные" not in text
    assert "неработающие" not in text
    assert "✅ Отправлено: 2" in text
    assert "⏳ Пропущены (<180 дней): 1" in text
    # ensure addresses listed with bullets
    assert "• a@example.com" in text
    assert "• c@example.com" in text
