create table effect_ledger (
  idempotency_key text primary key,
  payload jsonb not null,
  recorded_at timestamptz not null default now()
);
