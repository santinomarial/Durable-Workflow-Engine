alter table workflow_executions
  add column parent_workflow_id uuid references workflow_executions (id),
  add column parent_command_id bigint,
  add column parent_close_policy text,
  add constraint valid_parent_link check (
    (parent_workflow_id is null and parent_command_id is null and parent_close_policy is null)
    or
    (parent_workflow_id is not null and parent_command_id is not null
      and parent_command_id >= 0 and parent_close_policy in ('terminate', 'abandon'))
  );

create unique index one_child_per_parent_command
  on workflow_executions (parent_workflow_id, parent_command_id)
  where parent_workflow_id is not null;

create index open_children_by_parent
  on workflow_executions (parent_workflow_id, created_at)
  where parent_workflow_id is not null and status = 'running';

drop index one_scheduled_command;

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in (
      'ActivityScheduled', 'TimerStarted', 'MarkerRecorded', 'ChildWorkflowStarted'
    );
