-- Playbook Portal — Web Push subscriptions.
-- One row per browser/device a player has granted notification permission on
-- (a player using both a phone and a laptop gets two rows). Coaches send a
-- targeted notification (by position, or "All Team") from the upload page;
-- the server pushes to every matching subscription here.
-- Run once in the Supabase SQL editor.

create table if not exists playbook_push_subscriptions (
  id          uuid primary key default gen_random_uuid(),
  team_id     uuid not null references playbook_teams(id),
  email       text not null,
  endpoint    text not null unique,
  p256dh      text not null,
  auth        text not null,
  created_at  timestamptz not null default now()
);

create index if not exists playbook_push_subs_team_idx  on playbook_push_subscriptions (team_id);
create index if not exists playbook_push_subs_email_idx on playbook_push_subscriptions (email);

-- Server (service key) is the only writer/reader; enable RLS with no policies.
alter table playbook_push_subscriptions enable row level security;
