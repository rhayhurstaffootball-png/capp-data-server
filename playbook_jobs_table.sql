-- Playbook Portal — Visio conversion job queue.
-- A .vsd/.vsdx/.vsdm uploaded by the admin lands here as a "queued" job; a
-- Windows+Visio worker (or a LibreOffice fallback worker) claims it, converts it
-- to a PDF, uploads that to R2, and the finished PDF becomes a playbook_docs row.
-- Run once in the Supabase SQL editor.

create table if not exists playbook_jobs (
  id          uuid primary key default gen_random_uuid(),
  raw_key     text not null,              -- R2 key of the uploaded source Visio file
  ext         text,                       -- vsd / vsdx / vsdm
  folder_path text default '',            -- destination section path in the portal
  title       text not null,              -- display name (leaf, no extension)
  status      text not null default 'queued',  -- queued | converting | done | error
  error       text,                       -- failure message when status = error
  claimed_by  text,                       -- worker name that took the job
  claimed_at  timestamptz,
  out_key     text,                       -- R2 key allocated for the converted PDF
  doc_id      uuid,                        -- resulting playbook_docs row when done
  pages       int,
  size_bytes  bigint,
  created_at  timestamptz default now(),
  updated_at  timestamptz default now()
);

create index if not exists playbook_jobs_status_idx on playbook_jobs (status, created_at);

-- Atomic claim: hand exactly one queued job to a worker, skipping rows another
-- worker already locked. Prevents the PC worker and the fallback worker from
-- grabbing the same job. SECURITY DEFINER so the service role can call it.
create or replace function claim_playbook_job(p_worker text)
returns setof playbook_jobs
language plpgsql
security definer
as $$
begin
  return query
  update playbook_jobs j
     set status = 'converting',
         claimed_by = p_worker,
         claimed_at = now(),
         updated_at = now()
   where j.id = (
     select id from playbook_jobs
      where status = 'queued'
      order by created_at
      limit 1
      for update skip locked
   )
  returning j.*;
end;
$$;

-- Server (service key) is the only writer/reader; enable RLS with no policies.
alter table playbook_jobs enable row level security;
