import pytest

import config
import utils.email_clean as email_clean
from pipelines.extract_emails import run_pipeline_on_text


@pytest.fixture(autouse=True)
def enable_confusables(monkeypatch):
    monkeypatch.setattr(config, "CONFUSABLES_NORMALIZE", True, raising=False)
    monkeypatch.setattr(email_clean, "CONFUSABLES_NORMALIZE", True, raising=False)


def test_cyrillic_lookalikes_fixed():
    raw = "mail: mariа-smith@yаndex.ru"
    final, _ = run_pipeline_on_text(raw)
    assert "maria-smith@yandex.ru" in final
