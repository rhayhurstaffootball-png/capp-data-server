-- CAPP Binder — COACH-MANAGED GROUPS (Sep 10 2026)
--
-- Two things at once, both asked for by Roger:
--   1. A coach can create named groups of players (Scout Team, Freshmen,
--      OL + TE) and manage who is in them.
--   2. A folder can be limited to one or more of those groups, and then only
--      players in those groups see it in the playbook.
--
-- HOW VISIBILITY RESOLVES (the rule the server implements):
--   * A folder with NO row in playbook_folder_groups is visible to EVERYONE.
--     That is what makes this safe to deploy onto a live playbook - nothing
--     disappears until a coach deliberately restricts something.
--   * A restriction is INHERITED. The nearest folder at-or-above a path that
--     has any assignment decides it. A child with its own assignment overrides
--     whatever its parent says.
--   * Coaches, video staff and team admins are never filtered - they see the
--     whole tree, or they could not manage what they cannot see.
--
-- MEMBERS ARE KEYED BY EMAIL, NOT BY ROSTER ROW ID. A roster re-upload can
-- replace rows, and an id-keyed membership would quietly empty every group.
-- The email is the stable identity everywhere else in the Binder too - notes,
-- push subscriptions, the access log.
--
-- Run once in the Supabase SQL editor. Idempotent - safe to re-run.
--
-- WARNING carried over from page_review_migration.sql - the SQL editor splits
-- on a semicolon without respecting quotes, so no semicolon appears in this
-- file except as a real statement terminator, comments included.

create table if not exists playbook_groups (
  id         uuid primary key default gen_random_uuid(),
  team_id    uuid not null references playbook_teams(id),
  name       text not null,
  created_by text,
  created_at timestamptz not null default now()
);

-- One group name per team, case-insensitively. Two groups called "Scout Team"
-- and "scout team" would be indistinguishable in every list a coach reads.
create unique index if not exists playbook_groups_team_name_idx
  on playbook_groups (team_id, lower(name));

create table if not exists playbook_group_members (
  id         uuid primary key default gen_random_uuid(),
  team_id    uuid not null references playbook_teams(id),
  group_id   uuid not null references playbook_groups(id) on delete cascade,
  email      text not null,
  created_at timestamptz not null default now()
);

create unique index if not exists playbook_group_members_uniq
  on playbook_group_members (group_id, email);

-- The hot lookup - every manifest read asks "which groups is this email in".
create index if not exists playbook_group_members_team_email_idx
  on playbook_group_members (team_id, email);

create table if not exists playbook_folder_groups (
  id          uuid primary key default gen_random_uuid(),
  team_id     uuid not null references playbook_teams(id),
  folder_path text not null,
  group_id    uuid not null references playbook_groups(id) on delete cascade,
  created_at  timestamptz not null default now()
);

create unique index if not exists playbook_folder_groups_uniq
  on playbook_folder_groups (team_id, folder_path, group_id);

create index if not exists playbook_folder_groups_team_idx
  on playbook_folder_groups (team_id);

-- Server (service key) is the only reader and writer of all three, so RLS is
-- enabled with no policies at all - the anon key gets nothing.
alter table playbook_groups        enable row level security;

alter table playbook_group_members enable row level security;

alter table playbook_folder_groups enable row level security;

-- Verify:
--   select g.name, count(m.id) as members
--     from playbook_groups g
--     left join playbook_group_members m on m.group_id = g.id
--    group by g.id, g.name order by g.name
--
--   select folder_path, count(*) from playbook_folder_groups
--    group by folder_path order by folder_path
