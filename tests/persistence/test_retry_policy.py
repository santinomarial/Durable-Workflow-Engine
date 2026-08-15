from engine.persistence.transitions import retry_delay


def test_retry_delay_uses_capped_full_jitter() -> None:
    policy = {
        "initial_interval_seconds": 2.0,
        "backoff_coefficient": 3.0,
        "maximum_interval_seconds": 10.0,
    }

    assert retry_delay(policy, failed_attempt=1, random_value=0.5).total_seconds() == 1
    assert retry_delay(policy, failed_attempt=2, random_value=0.5).total_seconds() == 3
    assert retry_delay(policy, failed_attempt=3, random_value=0.5).total_seconds() == 5
