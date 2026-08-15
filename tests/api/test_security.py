import hashlib

import pytest

from engine.api.security import AuthConfig, FixedWindowRateLimiter


def test_authentication_is_fail_closed_without_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DWE_API_KEYS", raising=False)
    monkeypatch.delenv("DWE_AUTH_MODE", raising=False)

    config = AuthConfig.from_env()

    with pytest.raises(RuntimeError, match="authentication is required"):
        config.validate()


def test_hashed_environment_key_authenticates_without_storing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = "a-production-token-with-enough-entropy"
    digest = hashlib.sha256(token.encode()).hexdigest()
    monkeypatch.setenv("DWE_API_KEYS", f"operations:operator:{digest}")
    monkeypatch.setenv("DWE_RATE_LIMIT_PER_MINUTE", "42")

    config = AuthConfig.from_env()
    config.validate()

    assert config.requests_per_minute == 42
    assert config.authenticate(token) is not None
    assert config.authenticate(token).role == "operator"  # type: ignore[union-attr]
    assert config.authenticate("incorrect-token-value") is None
    assert token not in repr(config)


def test_rate_limiter_recovers_after_fixed_window() -> None:
    limiter = FixedWindowRateLimiter(2)

    assert limiter.allow("operator", now=1) == (True, 0)
    assert limiter.allow("operator", now=2) == (True, 0)
    allowed, retry_after = limiter.allow("operator", now=3)
    assert not allowed
    assert retry_after > 0
    assert limiter.allow("operator", now=62) == (True, 0)
