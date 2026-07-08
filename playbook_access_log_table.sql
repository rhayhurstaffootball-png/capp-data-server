-- Playbook Portal — permanent access log.
-- Every time a signed viewing URL is issued for a doc's PDF, a row lands here:
-- who (email), which doc, which team, when. This is an accountability record,
-- not an access CONTROL — it can't stop anyone, but it means every legitimate
-- in-app view is traceable. Requested by Roger (Jul 8 2026) after confirming
-- there is no admin-panel path to view a team's PDF content — the only doc-url
-- endpoint requires an actual rostered login for that team.
-- Run once in the Supabase SQL editor.

create table if not exists playbook_access_log (
  id         uuid primary key default gen_random_uuid(),
  team_id    uuid not null references playbook_teams(id),
  doc_id     uuid not null,
  email      text not null,
  created_at timestamptz not null default now()
);

create index if not exists playbook_access_log_team_idx on playbook_access_log (team_id, created_at desc);
create index if not exists playbook_access_log_doc_idx  on playbook_access_log (doc_id, created_at desc);

-- Server (service key) is the only writer/reader; enable RLS with no policies
-- so the anon key can never read the log either.
alter table playbook_access_log enable row level security;
