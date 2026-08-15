# Changelog

This project follows Semantic Versioning after `1.0.0`. During `0.x`, minor
versions may contain documented API changes while persisted-history and
migration compatibility remain explicit release requirements.

## [Unreleased]

### Added

- Fail-closed hashed bearer authentication with viewer/operator/admin roles.
- Immutable atomic API control-plane audits and bounded request protection.
- Production operations console with authenticated tab-scoped sessions.
- Liveness/readiness probes, Prometheus metrics, JSON logs, and worker heartbeats.
- Non-root locked container, hardened Compose reference, and secret-file config.
- Cursor-based history inspection, backup/restore tooling, and recovery guidance.
- Dependency, static, CodeQL, secret, misconfiguration, and container scanning.
- Indexed JSON search attributes with API filtering and operations-console editing.
- Durable pause/resume, dead-letter inspection, and linked retry-as-new recovery.
- Timezone-aware durable cron schedules with overlap policies and bounded backfills.
- Replay-native child workflows with atomic start, result propagation, and recursive parent-close policy.
- Deduplicated result-bearing workflow updates, typed handles, and snapshot query projections.
- Atomic continue-as-new with version handoff, inherited visibility metadata, and complete chain inspection.
- Interactive replay timeline with causal state frames and first-divergence command-trace comparison.

## [0.1.0] - 2026-08-15

### Added

- Deterministic replay engine with PostgreSQL atomic transitions.
- Activities, retries, timeouts, timers, signals, cancellation, and parallel joins.
- Immutable pinned definitions, replay compatibility checks, and marker values.
- Real `SIGKILL` chaos harness, benchmarks, packaging, API, CLI, and CI.
