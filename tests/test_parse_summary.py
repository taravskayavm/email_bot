from emailbot.ui.messages import format_dispatch_result, format_parse_summary
from emailbot.ui.keyboards import build_after_parse_combined_kb


def test_parse_summary_uses_disjoint_filter_counts() -> None:
    text = format_parse_summary(
        {
            "total_found": 150,
            "foreign_domain": 39,
            "invalid": 1,
            "blocked": 2,
            "blocked_after_parse": 2,
            "cooldown_180d": 42,
            "to_send": 66,
            "pages_skipped": 0,
        }
    )

    assert "Всего найдено: 150" in text
    assert "🌍 Иностранные домены: 39" in text
    assert "❌ Некорректные адреса: 1" in text
    assert "🚫 В стоп-листе: 2" in text
    assert "🚫 В стоп-листе: 4" not in text
    assert "⏳ Под кулдауном (180 дней): 42" in text
    assert "📦 К отправке: 66" in text
    assert "Пропущено страниц" not in text


def test_parse_summary_supports_legacy_blocked_key() -> None:
    text = format_parse_summary(
        {
            "total_found": 3,
            "foreign_domain": 0,
            "cooldown_180d": 0,
            "to_send": 2,
            "blocked": 1,
        }
    )

    assert "🚫 В стоп-листе: 1" in text


def test_after_parse_keyboard_has_only_actionable_entries() -> None:
    keyboard = build_after_parse_combined_kb()
    labels = [button.text for row in keyboard.inline_keyboard for button in row]

    assert "👀 Показать примеры" not in labels
    assert "🧭 Выбрать направление" in labels
    assert "✏️ Изменить список" in labels


def test_dispatch_summary_has_one_disjoint_line_per_outcome() -> None:
    text = format_dispatch_result(
        69,
        39,
        0,
        4,
        undeliverable=12,
        errors=9,
        pending=5,
        role_like=2,
    )

    assert "📊 В очереди было: 69" in text
    assert "🚫 В стоп-листе: 4" in text
    assert "👥 Служебные/редакционные адреса: 2" in text
    assert "📬 Недоставляемые: 12" in text
    assert "❌ Ошибок при отправке: 9" in text
    assert "⏸ Осталось в очереди: 5" in text
    assert "180 дней" not in text
