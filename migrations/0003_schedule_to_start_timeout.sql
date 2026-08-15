alter table tasks
  add column schedule_to_start_timeout interval;

alter table tasks
  add constraint positive_schedule_to_start_timeout
    check (schedule_to_start_timeout is null or schedule_to_start_timeout > interval '0 seconds');
