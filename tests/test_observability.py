import json
import logging

from engine.observability import JSONFormatter, MetricsRegistry


def test_json_formatter_preserves_context_without_log_noise() -> None:
    record = logging.LogRecord(
        name="engine.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="request complete",
        args=(),
        exc_info=None,
    )
    record.request_id = "request-123"
    record.actor = "oncall"

    payload = json.loads(JSONFormatter().format(record))

    assert payload["message"] == "request complete"
    assert payload["level"] == "INFO"
    assert payload["request_id"] == "request-123"
    assert payload["actor"] == "oncall"
    assert "pathname" not in payload


def test_metrics_registry_renders_valid_bounded_series() -> None:
    registry = MetricsRegistry()
    registry.increment("dwe_jobs_total", labels={"result": "ok"})
    registry.increment("dwe_jobs_total", labels={"result": "ok"}, value=2)
    registry.increment("dwe_jobs_total", labels={"result": "error"})
    registry.observe("dwe_job_duration_seconds", 0.02, labels={"role": "workflow"})

    rendered = registry.render(gauges={"dwe_workers_healthy": 2})

    assert rendered.count("# TYPE dwe_jobs_total counter") == 1
    assert 'dwe_jobs_total{result="ok"} 3' in rendered
    assert 'dwe_jobs_total{result="error"} 1' in rendered
    assert 'dwe_job_duration_seconds_count{role="workflow"} 1' in rendered
    assert "dwe_workers_healthy 2" in rendered
