alter table workflow_executions
  add column cancellation_requested_at timestamptz,
  add column cancellation_reason text;
