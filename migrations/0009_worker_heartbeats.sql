create table worker_heartbeats (
  worker_id uuid primary key,
  hostname text not null,
  process_id integer not null check (process_id > 0),
  queue_name text not null,
  roles text[] not null check (cardinality(roles) > 0),
  started_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  stopped_at timestamptz
);

create index active_worker_heartbeats
  on worker_heartbeats (last_seen_at desc)
  where stopped_at is null;
