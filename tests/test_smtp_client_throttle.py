import importlib
from typing import List

import pytest


class FakeTime:
    def __init__(self, start: float) -> None:
        self.now = start
        self.sleep_calls: List[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        self.sleep_calls.append(duration)
        self.now += duration

    def advance(self, delta: float) -> None:
        self.now += delta


def _reload_client(monkeypatch: pytest.MonkeyPatch, per_min: str, per_hour: str):
    monkeypatch.setenv("SMTP_MAX_PER_MIN", per_min)
    monkeypatch.setenv("SMTP_MAX_PER_HOUR", per_hour)
    module = importlib.import_module("utils.smtp_client")
    return importlib.reload(module)


@pytest.fixture(autouse=True)
def _restore_smtp_client_state():
    module = importlib.import_module("utils.smtp_client")
    orig_min = module.MAX_PER_MIN
    orig_hour = module.MAX_PER_HOUR
    yield
    module.MAX_PER_MIN = orig_min
    module.MAX_PER_HOUR = orig_hour
    module._TS_MIN.clear()
    module._TS_HOUR.clear()


def test_throttle_blocks_when_minute_limit_hit(monkeypatch: pytest.MonkeyPatch):
    module = _reload_client(monkeypatch, per_min="2", per_hour="10")
    fake = FakeTime(start=1000.0)
    monkeypatch.setattr(module, "time", fake)

    module._throttle_block()
    fake.advance(10)
    module._throttle_block()
    fake.advance(5)
    module._throttle_block()

    assert module.MAX_PER_MIN == 2
    assert fake.sleep_calls == [pytest.approx(45.0)]
    assert len(module._TS_MIN) == 2


def test_throttle_disabled_when_limits_zero(monkeypatch: pytest.MonkeyPatch):
    module = _reload_client(monkeypatch, per_min="0", per_hour="0")
    fake = FakeTime(start=2000.0)
    monkeypatch.setattr(module, "time", fake)

    for _ in range(3):
        module._throttle_block()

    assert fake.sleep_calls == []
    assert len(module._TS_MIN) == 0
    assert len(module._TS_HOUR) == 0
