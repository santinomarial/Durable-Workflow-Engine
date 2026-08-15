# Security policy

## Supported versions

Until the first stable release, only the latest commit on `main` and the latest
published `0.x` release receive security fixes. Older pre-1.0 releases may
contain incompatible workflow or migration behavior and should not be exposed.

## Report a vulnerability

Do not open a public issue containing exploit details, bearer tokens, database
URLs, workflow payloads, or customer data. Use GitHub's private vulnerability
reporting feature for this repository. Include the affected commit/version,
deployment shape, reproduction, impact, and any suggested mitigation.

The maintainer will acknowledge a report as soon as practical, validate its
scope, coordinate a fix and release, and credit the reporter when requested and
safe. There is no bug bounty or guaranteed response-time commitment.

## Security boundaries

- PostgreSQL, the API ingress, secret manager, and workflow/activity code are
  trusted deployment boundaries.
- Activities are at least once and external systems must deduplicate the stable
  idempotency key when exactly-once effects are required.
- Workflow code is not sandboxed. Only deploy reviewed definitions; put network,
  filesystem, clock, and random I/O in activities or recorded markers.
- Bearer tokens require TLS and should be rotated through a secret manager.
- The reference Compose file is a hardened single-host example, not a claim of
  database high availability or tenant isolation.

Security fixes that alter durable semantics must preserve old pinned workflow
versions or provide an explicit compatibility and rollout plan.
