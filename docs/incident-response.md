# Incident response runbook

## Triage

1. Declare an incident owner, start a timestamped log, and record the deployed
   image digest, package version, database primary, and affected queues.
2. Preserve API/worker logs, metrics, database audit rows, and relevant workflow
   IDs. Never paste bearer tokens or payloads into the incident channel.
3. Check API liveness/readiness, healthy worker heartbeats by queue/role, HTTP
   error rate, pending/leased/dead task counts, database saturation, replication
   state, and recent deploy/migration/key changes.

## Containment

- Stop API ingress to prevent new starts/signals while allowing workers to drain
  when ingestion is the source of harm.
- Scale or stop a specific queue's workers when activity code is unsafe. Lease
  fencing prevents late completions after expiry; external effects may still
  need their own containment.
- Revoke a bearer key by removing its digest and rolling API replicas. Use audit
  `actor_key_id` plus request IDs to scope actions taken by the key.
- Do not manually delete history or rewrite migration rows. Take a backup before
  any emergency database operation.

## Recovery

1. Prefer a forward fix that retains pinned workflow code and schema support.
2. Run `replay-check` on affected histories before enabling new code.
3. Restore only through the isolated restore procedure when the database is
   corrupted or a planned point-in-time recovery is required.
4. Re-enable one worker role/queue at a time, watch task and error metrics, then
   reopen API ingestion.
5. Verify representative workflow results and idempotency-ledger boundaries.

## Closure

Rotate exposed credentials, publish a timeline and impact assessment, capture
the triggering gap, add a regression or chaos test, update alerts/runbooks, and
track every remediation to an owner. Preserve evidence according to the
deployment's retention and legal requirements.
