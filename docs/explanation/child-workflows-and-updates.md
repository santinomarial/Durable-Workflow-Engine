# Child workflows and result-bearing updates

## Child workflows

`ctx.child_workflow` records a deterministic child command in parent history
and creates the child execution, its initial history, and its task in the
same database transaction — the same atomicity that backs every other
workflow transition (see
[transaction and lease invariants](replay-and-determinism.md#transaction-and-lease-invariants)).
The child pins its own definition version and may use the parent's queue or
an explicit queue. Its terminal transition appends the corresponding child
result/failure event to the parent and wakes parent replay atomically — the
parent never polls for the child; it's woken by the same commit that closes
the child.

The `parent_close_policy` exists because "what happens to a child when its
parent closes" has no single correct answer:

- `terminate` (the default) recursively terminates open descendants and
  fences their outstanding tasks. Use this when the child's lifecycle is
  conceptually owned by the parent.
- `abandon` intentionally leaves a child running after the parent closes.
  Use this only when the child owns an independent business lifecycle —
  otherwise a crashed or completed parent silently orphans real work.

Child external effects retain the same at-least-once and idempotency
requirements as every other activity — a child workflow is not a different
execution model, just a different join target.

## Result-bearing updates

Updates exist because signals are one-way: a caller who needs to know
whether their request was accepted, and with what result, can't get that
from a signal alone. `ctx.wait_update()` and `ctx.resolve_update()` (see the
[SDK reference](../reference/sdk-api.md#result-bearing-updates)) record
update receipt and resolution as separate deterministic events, so a caller
can poll for `pending`, `completed`, or `rejected` and get back either the
workflow-produced result or a failure. Closing a workflow rejects any
unresolved updates rather than leaving callers waiting forever — an update
handle is a promise the engine keeps, even in the failure path.

## Continue-as-new

Long-lived top-level workflows need to rotate into a fresh, pinned run
without a race between closing the old run and starting the new one. The
engine records `WorkflowExecutionContinuedAsNew`, closes the old execution,
and creates the new execution and its first task in the same transaction, so
there is never a window where neither run is the "current" one. It inherits
search attributes and schedule identity and exposes `continued_from` /
`continued_to` plus `GET /api/workflows/{id}/continuation-chain` so operators
and callers can still navigate the whole chain as one logical workflow.

Child workflows cannot yet continue as new, because a parent's join identity
is bound to the specific child execution it started — continuing that child
into a new execution would break the parent's ability to recognize its
terminal event. Making that safe needs chain-aware parent completion
semantics, tracked as a [known limitation](guarantees-and-limitations.md).
