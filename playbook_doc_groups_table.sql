-- CAPP Binder: limit ONE FILE to player groups (Sep 24 2026).
-- Roger: the Nevada itinerary inside the hotel binder must be visible to the
-- Travel Playmakers group only, without touching the folder it sits in.
--
-- Rule (mirrors playbook_folder_groups, see playbook_groups_table.sql):
--   * A doc with NO row here follows its folder's rule (everyone, or the
--     folder's groups).
--   * A doc WITH rows here is visible only to members of those groups, AND
--     still only if its folder is visible to them.
--   * Staff (coach, video, team admin) are never filtered.
--   * If this table is missing the server fails OPEN, exactly like folders.
--
-- HOW TO RUN: paste into the Supabase SQL editor, press Ctrl+A to select
-- everything, THEN Run. The editor runs only the selected text and this file
-- opens with comments, so an unselected run reports success and does nothing.
-- No semicolon appears anywhere below except as a statement terminator.

create table if not exists playbook_doc_groups (
  id         uuid primary key default gen_random_uuid(),
  team_id    uuid not null references playbook_teams(id),
  doc_id     uuid not null references playbook_docs(id) on delete cascade,
  group_id   uuid not null references playbook_groups(id) on delete cascade,
  created_at timestamptz not null default now()
);

create unique index if not exists playbook_doc_groups_uniq
  on playbook_doc_groups (team_id, doc_id, group_id);

create index if not exists playbook_doc_groups_team_idx
  on playbook_doc_groups (team_id);

alter table playbook_doc_groups enable row level security;

-- Verify (run separately): the table answers with HTTP 200 on
--   GET {SUPABASE_URL}/rest/v1/playbook_doc_groups?select=id&limit=1
-- with the service key. PGRST205 means it was not created.
