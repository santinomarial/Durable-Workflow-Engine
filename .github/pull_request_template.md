## Change

Describe the behavior and operational impact.

## Durable compatibility

- [ ] Existing workflow histories replay against unchanged pinned versions.
- [ ] Migration changes are forward-only, transactional, and checksum-stable.
- [ ] Retry, timeout, cancellation, and duplicate-delivery behavior were considered.

## Verification

- [ ] Formatting, lint, typing, security audit, and unit tests pass.
- [ ] PostgreSQL integration tests pass when persistence changed.
- [ ] Chaos/recovery evidence was updated when a failure boundary changed.
- [ ] Documentation, configuration, metrics, and runbooks were updated.

## Deployment

State rollout order, rollback constraints, feature/config flags, and monitoring signals.
