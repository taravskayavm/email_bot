"""Basic coverage for the domain rate limiter planner."""

from mailer.smtp_sender import DomainRateLimiter


def test_rate_limit_basic() -> None:
    limiter = DomainRateLimiter(limit_per_min=2)
    send_now, deferred, increments = limiter.plan(
        ["a@x.com", "b@x.com", "c@x.com", "d@y.com"]
    )

    assert set(send_now) == {"a@x.com", "b@x.com", "d@y.com"}
    assert deferred == ["c@x.com"]
    assert increments["x.com"] == 2
    assert increments["y.com"] == 1

    limiter.commit(increments)
