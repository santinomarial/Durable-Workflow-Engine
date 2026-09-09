# Back up and restore

Durable history is the source of truth. A production backup policy must cover
the entire PostgreSQL database — including execution, history, task,
definition, effect-ledger, audit, heartbeat, and migration tables — as one
consistency unit.

## Take a logical backup

The bundled script creates a PostgreSQL custom-format backup with owner and
privilege metadata removed, mode `0600`, and a SHA-256 checksum:

```shell
DATABASE_URL='postgresql://...' scripts/backup.sh /secure/durable.dump
sha256sum --check /secure/durable.dump.sha256
```

Transfer the dump and checksum to encrypted, access-controlled, immutable
storage. Logical backups are useful for portability and release checkpoints;
they are not a substitute for continuous WAL archiving. Business-critical
installations should enable encrypted base backups plus point-in-time
recovery and define an RPO/RTO with their database operator.

## Restore into an isolated database

Always restore into an isolated database first — never directly onto a
database anything else is using. The restore script intentionally requires a
distinct URL and an exact confirmation phrase, because it drops and replaces
objects in the target database:

```shell
export RESTORE_DATABASE_URL='postgresql://.../durable_restore_test'
export DWE_RESTORE_CONFIRM=replace-target-database
scripts/restore.sh /secure/durable.dump
```

After restoration:

1. Confirm the reported schema version matches the backup's application
   release, then start the matching engine package.
2. Compare row counts and terminal/running status totals with the backup
   manifest or monitoring data.
3. Run `engine replay-check` against representative running and terminal
   executions without mutation.
4. Start one maintenance worker and verify timers, lease recovery, and worker
   heartbeat state before enabling activity workers.
5. Expose the API only after readiness succeeds and operators approve
   cutover.

## Practice the drill on a schedule

A backup that has never completed a restore drill is not recovery evidence.
Run the procedure above against a real dump on a recurring cadence, not just
when you first set it up.

For why history is append-only and never safe to delete from a live database,
see [data and history lifecycle](../explanation/data-and-history-lifecycle.md).
