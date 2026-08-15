alter table workflow_executions
  add column search_attributes jsonb not null default '{}'::jsonb,
  add constraint search_attributes_are_object
    check (jsonb_typeof(search_attributes) = 'object'),
  add constraint search_attributes_are_bounded
    check (pg_column_size(search_attributes) <= 16384);

create index executions_by_search_attributes
  on workflow_executions using gin (search_attributes jsonb_path_ops);

create index executions_by_type_created
  on workflow_executions (workflow_type, created_at desc);

create index executions_by_queue_created
  on workflow_executions (queue_name, created_at desc);
