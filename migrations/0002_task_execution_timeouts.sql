alter table tasks
  add column start_to_close_timeout interval;

alter table tasks
  add constraint positive_heartbeat_timeout
    check (heartbeat_timeout is null or heartbeat_timeout > interval '0 seconds'),
  add constraint positive_start_to_close_timeout
    check (start_to_close_timeout is null or start_to_close_timeout > interval '0 seconds');
