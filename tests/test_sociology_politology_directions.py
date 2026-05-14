from pathlib import Path

import emailbot.bot_handlers as bot_handlers
from emailbot import messaging


class DummyContext:
    """Minimal context object for keyboard metadata storage."""

    def __init__(self):
        self.user_data = {}


def _button_texts(markup):
    """Return all button labels from a Telegram inline keyboard."""

    return [button.text for row in markup.inline_keyboard for button in row]


def test_sociology_and_politology_registered_templates():
    """New direction codes should resolve to their existing HTML templates."""

    assert messaging.TEMPLATE_MAP["sociology"].endswith("templates/sociology.html")
    assert messaging.TEMPLATE_MAP["politology"].endswith("templates/politology.html")


def test_runtime_direction_labels_include_icons():
    """Runtime labels should include icons used by the actual group keyboard."""

    assert (
        bot_handlers._direction_button_label("sociology", "Социология")
        == "🏛️ Социология"
    )
    assert (
        bot_handlers._direction_button_label("politology", "Политология")
        == "🗳️ Политология"
    )


def test_build_group_markup_renders_new_direction_icons(monkeypatch):
    """The runtime group keyboard should render new directions with icons."""

    templates_dir = Path("templates").resolve()
    monkeypatch.setattr(
        bot_handlers,
        "list_templates",
        lambda: [
            {
                "code": "sociology",
                "label": "Социология",
                "path": str(templates_dir / "sociology.html"),
            },
            {
                "code": "politology",
                "label": "Политология",
                "path": str(templates_dir / "politology.html"),
            },
        ],
    )

    markup = bot_handlers._build_group_markup(DummyContext(), prefix="group_")

    assert "🏛️ Социология" in _button_texts(markup)
    assert "🗳️ Политология" in _button_texts(markup)
