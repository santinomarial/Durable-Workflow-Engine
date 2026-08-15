create extension if not exists pgcrypto;

create type workflow_status as enum (
  'running',
  'completed',
  'failed',
  'terminated'
);

create type task_status as enum (
  'pending',
  'leased',
  'completed',
  'dead'
);

create type task_type as enum (
  'workflow',
  'activity',
  'timer'
);

create table workflow_definitions (
  workflow_type text not null,
  version integer not null check (version > 0),
  code_hash text not null,
  registered_at timestamptz not null default now(),
  primary key (workflow_type, version)
);

create function reject_definition_mutation()
returns trigger
language plpgsql
as $$
begin
  raise exception 'workflow definitions are immutable';
end;
$$;

create trigger immutable_workflow_definitions
before update or delete on workflow_definitions
for each row execute function reject_definition_mutation();

create table workflow_executions (
  id uuid primary key,
  workflow_type text not null,
  definition_version integer not null,
  input jsonb not null,
  status workflow_status not null default 'running',
  result jsonb,
  failure jsonb,
  next_seq bigint not null default 1 check (next_seq > 0),
  created_at timestamptz not null default now(),
  closed_at timestamptz,
  foreign key (workflow_type, definition_version)
    references workflow_definitions (workflow_type, version),
  check (
    (status = 'running' and closed_at is null and result is null and failure is null)
    or (status = 'completed' and closed_at is not null and failure is null)
    or (status in ('failed', 'terminated') and closed_at is not null and result is null)
  )
);

create table history_events (
  workflow_id uuid not null references workflow_executions (id),
  seq bigint not null check (seq > 0),
  event_type text not null,
  command_id bigint check (command_id is null or command_id >= 0),
  entity_id uuid,
  external_id text,
  attributes jsonb not null,
  created_at timestamptz not null default now(),
  primary key (workflow_id, seq)
);

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in ('ActivityScheduled', 'TimerStarted');

create unique index one_external_event
  on history_events (workflow_id, external_id)
  where external_id is not null;

create index history_by_entity
  on history_events (workflow_id, entity_id, seq)
  where entity_id is not null;

create table tasks (
  id uuid primary key,
  workflow_id uuid not null references workflow_executions (id),
  task_type task_type not null,
  queue_name text not null default 'default',
  entity_id uuid,
  command_id bigint check (command_id is null or command_id >= 0),
  attempt integer not null default 1 check (attempt > 0),
  input jsonb,
  status task_status not null default 'pending',
  visible_at timestamptz not null default now(),
  schedule_to_start_deadline timestamptz,
  start_to_close_deadline timestamptz,
  heartbeat_timeout interval,
  leased_at timestamptz,
  lease_token uuid,
  lease_expires_at timestamptz,
  heartbeat_at timestamptz,
  heartbeat_details jsonb,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  check (
    (status = 'pending' and leased_at is null and lease_token is null
      and lease_expires_at is null and completed_at is null)
    or (status = 'leased' and leased_at is not null and lease_token is not null
      and lease_expires_at is not null and completed_at is null)
    or (status in ('completed', 'dead') and completed_at is not null)
  ),
  check (
    (task_type = 'workflow' and entity_id is null and command_id is null)
    or (task_type in ('activity', 'timer') and entity_id is not null
      and command_id is not null)
  )
);

create unique index one_entity_attempt
  on tasks (workflow_id, task_type, entity_id, attempt)
  where entity_id is not null;

create index runnable_tasks
  on tasks (queue_name, task_type, visible_at, created_at)
  where status = 'pending';

create index expired_leases
  on tasks (lease_expires_at)
  where status = 'leased';
