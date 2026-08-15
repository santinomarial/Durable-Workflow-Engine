create type workflow_update_status as enum ('pending', 'completed', 'rejected');

create table workflow_updates (
  workflow_id uuid not null references workflow_executions (id),
  update_id text not null,
  name text not null,
  payload jsonb not null,
  status workflow_update_status not null default 'pending',
  result jsonb,
  failure jsonb,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  primary key (workflow_id, update_id),
  check (length(update_id) between 1 and 200),
  check (length(name) between 1 and 200),
  check (
    (status = 'pending' and result is null and failure is null and completed_at is null)
    or (status = 'completed' and failure is null and completed_at is not null)
    or (status = 'rejected' and result is null and failure is not null and completed_at is not null)
  )
);

create index pending_workflow_updates
  on workflow_updates (workflow_id, created_at)
  where status = 'pending';

drop index one_scheduled_command;

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in (
      'ActivityScheduled', 'TimerStarted', 'MarkerRecorded',
      'ChildWorkflowStarted', 'WorkflowUpdateResolved'
    );
