create type schedule_overlap_policy as enum ('allow', 'skip', 'buffer_one');
create type schedule_occurrence_status as enum ('started', 'skipped', 'buffered');

create table workflow_schedules (
  id uuid primary key,
  name text not null unique,
  workflow_type text not null,
  definition_version integer not null,
  input jsonb not null,
  queue_name text not null,
  search_attributes jsonb not null default '{}'::jsonb,
  cron_expression text not null,
  timezone text not null default 'UTC',
  overlap_policy schedule_overlap_policy not null default 'skip',
  next_run_at timestamptz not null,
  last_run_at timestamptz,
  paused_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  foreign key (workflow_type, definition_version)
    references workflow_definitions (workflow_type, version),
  check (length(name) between 1 and 200),
  check (length(queue_name) between 1 and 200),
  check (jsonb_typeof(search_attributes) = 'object'),
  check (pg_column_size(search_attributes) <= 16384)
);

create index due_workflow_schedules
  on workflow_schedules (next_run_at, id)
  where paused_at is null;

alter table workflow_executions
  add column schedule_id uuid references workflow_schedules (id),
  add column scheduled_at timestamptz;

create index executions_by_schedule
  on workflow_executions (schedule_id, created_at desc)
  where schedule_id is not null;

create table schedule_occurrences (
  schedule_id uuid not null references workflow_schedules (id),
  scheduled_at timestamptz not null,
  status schedule_occurrence_status not null,
  workflow_id uuid references workflow_executions (id),
  reason text,
  created_at timestamptz not null default now(),
  primary key (schedule_id, scheduled_at),
  check ((status = 'started' and workflow_id is not null)
    or (status <> 'started' and workflow_id is null))
);

create unique index one_buffered_occurrence_per_schedule
  on schedule_occurrences (schedule_id)
  where status = 'buffered';
