do $$
declare
  status_type regtype;
  status_constraint record;
begin
  select a.atttypid::regtype
    into status_type
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'public'
    and t.relname = 'drive_pdf_jobs'
    and a.attname = 'status'
    and not a.attisdropped;

  if status_type is null then
    raise exception 'public.drive_pdf_jobs.status column was not found';
  end if;

  if exists (
    select 1
    from pg_type t
    where t.oid = status_type
      and t.typtype = 'e'
  ) then
    execute format('alter type %s add value if not exists %L', status_type, 'cancelled');
  end if;

  for status_constraint in
    select c.conname
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'drive_pdf_jobs'
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%status%'
      and pg_get_constraintdef(c.oid) ilike '%queued%'
      and pg_get_constraintdef(c.oid) ilike '%skipped%'
  loop
    execute format('alter table public.drive_pdf_jobs drop constraint %I', status_constraint.conname);
  end loop;

  alter table public.drive_pdf_jobs
    add constraint drive_pdf_jobs_status_check
    check (status in (
      'queued',
      'downloading',
      'compressing',
      'uploading',
      'completed',
      'failed',
      'skipped',
      'cancelled'
    ));
end $$;
