alter table workflow_executions
  add column paused_at timestamptz,
  add column pause_reason text,
  add column retry_of uuid references workflow_executions (id);

create index paused_executions
  on workflow_executions (paused_at, created_at desc)
  where paused_at is not null and status = 'running';

create index execution_retry_chain
  on workflow_executions (retry_of, created_at)
  where retry_of is not null;
