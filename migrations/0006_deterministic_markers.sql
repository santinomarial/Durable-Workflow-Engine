drop index one_scheduled_command;

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in ('ActivityScheduled', 'TimerStarted', 'MarkerRecorded');
