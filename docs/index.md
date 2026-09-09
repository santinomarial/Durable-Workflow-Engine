# Documentation map

This documentation follows the [Diátaxis](https://diataxis.fr) framework:
four kinds of writing for four different things a reader needs.

## [Tutorial](tutorials/getting-started.md)

Learning-oriented. Start here if you're new — it walks you through running
the engine end to end, one guided path, no decisions to make.

## How-to guides

Goal-oriented recipes for someone who already knows what they're trying to
do:

- [Deploy to production](how-to/deploy-to-production.md)
- [Monitor and operate](how-to/monitor-and-operate.md)
- [Back up and restore](how-to/back-up-and-restore.md)
- [Respond to an incident](how-to/respond-to-an-incident.md)
- [Provision and rotate API keys](how-to/provision-and-rotate-api-keys.md)
- [Run the chaos suite](how-to/run-the-chaos-suite.md)
- [Run benchmarks](how-to/run-benchmarks.md)
- [Release a version](how-to/release-a-version.md)

## Reference

Information-oriented lookup material — dry, precise, no narrative:

- [CLI reference](reference/cli.md)
- [Configuration reference](reference/configuration.md)
- [API and roles reference](reference/api-and-roles.md)
- [Metrics and log fields reference](reference/metrics-and-logs.md)
- [Benchmark results](reference/benchmark-results.md)
- [Python SDK reference](reference/sdk-api.md)

## Explanation

Understanding-oriented — the reasoning behind how the engine works and what
it does and doesn't guarantee:

- [Replay and determinism](explanation/replay-and-determinism.md)
- [Guarantees and limitations](explanation/guarantees-and-limitations.md)
- [Data and history lifecycle](explanation/data-and-history-lifecycle.md)
- [Child workflows and result-bearing updates](explanation/child-workflows-and-updates.md)
- [Chaos methodology](explanation/chaos-methodology.md)
- [Benchmark analysis](explanation/benchmark-analysis.md)

## Design

- [Implementation blueprint](design/implementation-plan.md) — the original
  design rationale and milestone acceptance criteria. Historical/maintainer
  reference, not part of the four sections above.
