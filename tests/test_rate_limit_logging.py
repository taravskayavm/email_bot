import logging
from emailbot.messaging import log_domain_rate_limit


def test_log_domain_rate_limit(caplog):
    caplog.set_level(logging.INFO)
    # не спим в тесте: передаём sleep_s=0
    log_domain_rate_limit("example.com", 0)
    assert any("rate-limit" in rec.message for rec in caplog.records)
    assert any("example.com" in rec.message for rec in caplog.records)
