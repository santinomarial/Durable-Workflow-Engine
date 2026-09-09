# Data and history lifecycle

History and API audit records are append-only and are not silently deleted
by the service. **Do not delete history rows from a live database — that
breaks deterministic replay** (see
[replay and determinism](replay-and-determinism.md)).

## Why replay needs the whole history

Workflow replay reads a complete history for each run because snapshots and
archival are semantic engine features, not safe housekeeping deletions —
removing a row removes a fact the replay model depends on to reproduce past
decisions. This is why continue-as-new exists as a workflow-authored escape
hatch: `ctx.continue_as_new(...)` atomically closes the current run and
starts a linked fresh history, and the inspection API exposes the entire
chain, so a long-lived workflow can bound its own history length without
losing continuity for callers.

Use the published replay and queue-depth benchmarks (see
[benchmark results](../reference/benchmark-results.md)) to set workload
admission limits, alert on per-run history growth, and continue-as-new before
the published envelope, rather than after a run has already grown
unmanageable.

## Why inspection is paginated

The HTTP history endpoint uses a monotonic sequence cursor with a maximum
page size of 1,000. The console loads a bounded prefix plus the latest tail
and explicitly marks omitted middle history, instead of exhausting browser
memory trying to render an arbitrarily long run.
