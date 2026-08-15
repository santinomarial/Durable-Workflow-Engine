alter table workflow_executions
  add column queue_name text not null default 'default';
