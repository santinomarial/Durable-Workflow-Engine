create table api_audit_log (
  id bigint generated always as identity primary key,
  occurred_at timestamptz not null default now(),
  request_id uuid not null,
  actor_key_id text not null,
  actor_role text not null check (actor_role in ('viewer', 'operator', 'admin')),
  action text not null,
  workflow_id uuid references workflow_executions (id),
  accepted boolean not null,
  details jsonb not null default 'null'::jsonb
);

create index api_audit_by_actor_time
  on api_audit_log (actor_key_id, occurred_at desc);

create index api_audit_by_workflow_time
  on api_audit_log (workflow_id, occurred_at desc)
  where workflow_id is not null;

create function reject_api_audit_mutation()
returns trigger
language plpgsql
as $$
begin
  raise exception 'API audit records are immutable';
end;
$$;

create trigger immutable_api_audit_log
before update or delete on api_audit_log
for each row execute function reject_api_audit_mutation();
