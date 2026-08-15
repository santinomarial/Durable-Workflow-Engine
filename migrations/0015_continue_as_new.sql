alter table workflow_executions
  add column continued_from uuid references workflow_executions (id),
  add column continued_to uuid references workflow_executions (id),
  add constraint continuation_is_not_self
    check (continued_from is null or continued_from <> id),
  add constraint continuation_target_is_not_self
    check (continued_to is null or continued_to <> id);

create unique index one_continuation_from_execution
  on workflow_executions (continued_from)
  where continued_from is not null;

create unique index one_continuation_target
  on workflow_executions (continued_to)
  where continued_to is not null;

drop index one_scheduled_command;

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in (
      'ActivityScheduled', 'TimerStarted', 'MarkerRecorded',
      'ChildWorkflowStarted', 'WorkflowUpdateResolved',
      'WorkflowExecutionContinuedAsNew'
    );
