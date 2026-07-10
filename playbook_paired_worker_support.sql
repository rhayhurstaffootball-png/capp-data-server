-- CAPP Binder — paired local conversion workers.
-- Replaces the single shared PB_WORKER_TOKEN / "any worker takes any job"
-- model with: each coach pairs THEIR OWN computer to THEIR OWN login. From
-- then on, a job is claimed only by the worker paired to whoever uploaded it
-- — never by another coach's machine, even on the same team. See
-- "T:\BINDER LOCAL PLAN.txt" for the full design.
-- Run once in the Supabase SQL editor. Additive + idempotent.

-- One row per paired computer. `token` is that device's own private worker
-- credential (replaces PB_WORKER_TOKEN for paired workers).
create table if not exists playbook_converter_devices (
  id            uuid primary key default gen_random_uuid(),
  email         text not null,             -- the coach login this device is paired to
  team_id       uuid not null,
  device_name   text,                      -- hostname, for the coach to recognize it later
  token         text not null unique,      -- this device's own worker credential
  paired_at     timestamptz default now(),
  last_seen_at  timestamptz
);
create index if not exists playbook_converter_devices_email_idx on playbook_converter_devices (email);

-- Short-lived one-time tokens the browser mints so the downloaded setup can
-- pair itself without the coach re-entering credentials. Consumed on first use.
create table if not exists playbook_converter_pairing_tokens (
  token       text primary key,
  email       text not null,
  team_id     uuid not null,
  created_at  timestamptz default now(),
  used_at     timestamptz
);

-- Which login uploaded each job — the claim is scoped to THIS, not to team_id,
-- so two coaches on the same team never share a worker.
alter table playbook_jobs add column if not exists uploader_email text;

-- Server (service key) is the only writer/reader; enable RLS with no policies,
-- matching playbook_jobs.
alter table playbook_converter_devices enable row level security;
alter table playbook_converter_pairing_tokens enable row level security;

-- Scoped claim: a legacy (shared-token) worker only ever gets jobs with NO
-- uploader (today that's just admin-panel-direct uploads); a paired worker
-- only ever gets jobs uploaded by ITS OWN paired login — never another
-- coach's files, even on the same team. p_uploader_email = NULL means
-- "legacy worker" (matches uploader_email IS NULL rows only).
create or replace function claim_playbook_job_scoped(p_worker text, p_uploader_email text)
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
        and (
          (p_uploader_email is null and uploader_email is null)
          or (p_uploader_email is not null and uploader_email = p_uploader_email)
        )
      order by created_at
      limit 1
      for update skip locked
   )
  returning j.*;
end;
$$;
