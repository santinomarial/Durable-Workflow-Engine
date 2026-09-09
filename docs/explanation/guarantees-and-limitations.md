# Guarantees and limitations

This repository implements the mechanisms behind durable execution; it does
not claim to replace Temporal.

## What's guaranteed, within one PostgreSQL instance

- ordered, append-only history per workflow;
- atomic history, task, and execution-state transitions;
- deterministic replay with command fingerprints and recorded marker values
  (see [replay and determinism](replay-and-determinism.md));
- exclusive temporary leases and token-fenced completion;
- at-least-once activities with a stable idempotency key across attempts;
- durable timers, signals, retries, timeouts, cancellation requests, and
  joins;
- pinned immutable workflow versions and non-mutating compatibility checks;
  and
- no accepted transition after completion, failure, or termination.

The central guarantee is:

> For every accepted workflow transition, its history events and runnable
> tasks commit atomically, and replaying the committed history produces the
> same next commands.

## What's explicitly not guaranteed

- **Exactly-once arbitrary side effects.** An activity can perform an
  external effect and die before recording completion. Exactly-once behavior
  exists only when that external boundary atomically deduplicates the
  engine-provided idempotency key.
- **Multi-region storage, sharding, or multi-tenancy.** PostgreSQL is a
  single coordination boundary; there is no sharding, multi-region failover,
  or independent message broker.
- **Automatic migration of running workflow code.** Old pinned code must stay
  deployed until no execution references it.
- **Tenant isolation.** Control-plane authentication protects API access; it
  is not tenant isolation.

## Known limitations

- Worker replay still loads one run's history in full. Continue-as-new
  bounds individual runs, but replay snapshots and automated history
  archival are not implemented. Inspection APIs are cursor-paginated.
- Continue-as-new is currently restricted to top-level workflows; child-run
  continuation needs chain-aware parent completion semantics.
- The API/UI provide hashed bearer-key authentication, role authorization,
  request limits, and immutable mutation audits, but not tenant isolation.
  TLS termination and shared multi-replica rate limiting belong at the
  ingress.
- Workflow sandboxing is documented but not enforced by bytecode or process
  isolation; authors must keep workflow code deterministic and put I/O in
  activities.
- Cancellation cannot forcibly stop or undo external work.
- There are no cross-language SDKs or automatic workflow-code migrations.
- No software license has been selected.

The detailed design rationale and milestone acceptance criteria are in the
[implementation blueprint](../design/implementation-plan.md).
