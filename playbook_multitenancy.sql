-- ============================================================================
-- CAPP Binder — MULTI-TENANCY MIGRATION (Step 1: data model)
-- Written Jul 8 2026.  Plan: docs/BINDER_MULTITENANCY_PLAN.md
--
-- WHAT THIS DOES (additive + non-destructive):
--   1. Creates the new `playbook_teams` table.
--   2. Inserts the first team = "Air Force" (slug 'airforce').
--   3. Adds a `team_id` label to playbook_users / _docs / _folders / _jobs and
--      backfills EVERY existing row to Air Force, then locks it NOT NULL.
--   4. Adds `is_admin` to playbook_users (all false; Team Admin role).
--
-- WHAT IT DOES NOT TOUCH:
--   - No PDF bytes (those live in R2, untouched).
--   - No passwords / pw_hash / pw_salt.
--   - playbook_notes (Touch Notes) — left exactly as-is; notes are implicitly
--     team-scoped via email (globally unique) + doc_id.
--
-- SAFETY: idempotent (safe to re-run) and wrapped in a single transaction — if
-- anything fails, the whole thing rolls back and the DB is unchanged. A full
-- JSON backup of all Binder tables was taken first:
--   T:\capp-data-server\_backups\binder_20260708_111620\
--
-- HOW TO RUN: paste into the Supabase SQL editor and Run (one shot).
-- ============================================================================

begin;

-- ----------------------------------------------------------------------------
-- 1. Teams table
-- ----------------------------------------------------------------------------
create table if not exists playbook_teams (
  id          uuid primary key default gen_random_uuid(),
  slug        text unique not null,            -- stable id, e.g. 'airforce'
  name        text not null,                   -- display, e.g. 'Air Force Falcons'
  logo_r2_key text,                            -- team logo object in R2 (nullable)
  active      boolean not null default true,
  created_at  timestamptz default now()
);

-- Server uses the service key (bypasses RLS); enable RLS with no policy so the
-- anon key cannot read the teams list. (Same pattern as the other tables.)
alter table playbook_teams enable row level security;

-- ----------------------------------------------------------------------------
-- 2. Seed the first team = Air Force (idempotent)
-- ----------------------------------------------------------------------------
insert into playbook_teams (slug, name)
values ('airforce', 'Air Force')
on conflict (slug) do nothing;

-- ----------------------------------------------------------------------------
-- 3. Add team_id to every team-scoped table, backfill to Air Force, lock NOT NULL
--    (add nullable -> backfill -> set NOT NULL, so existing rows never violate)
-- ----------------------------------------------------------------------------
alter table playbook_users   add column if not exists team_id uuid;
alter table playbook_docs     add column if not exists team_id uuid;
alter table playbook_folders  add column if not exists team_id uuid;
alter table playbook_jobs     add column if not exists team_id uuid;

-- Backfill: every existing row belongs to Air Force.
update playbook_users   set team_id = (select id from playbook_teams where slug='airforce') where team_id is null;
update playbook_docs     set team_id = (select id from playbook_teams where slug='airforce') where team_id is null;
update playbook_folders  set team_id = (select id from playbook_teams where slug='airforce') where team_id is null;
update playbook_jobs     set team_id = (select id from playbook_teams where slug='airforce') where team_id is null;

-- Lock NOT NULL now that no row is null.
alter table playbook_users   alter column team_id set not null;
alter table playbook_docs     alter column team_id set not null;
alter table playbook_folders  alter column team_id set not null;
alter table playbook_jobs     alter column team_id set not null;

-- Foreign keys (idempotent via pg_constraint check).
do $$ begin
  if not exists (select 1 from pg_constraint where conname='playbook_users_team_fk') then
    alter table playbook_users  add constraint playbook_users_team_fk  foreign key (team_id) references playbook_teams(id);
  end if;
  if not exists (select 1 from pg_constraint where conname='playbook_docs_team_fk') then
    alter table playbook_docs    add constraint playbook_docs_team_fk    foreign key (team_id) references playbook_teams(id);
  end if;
  if not exists (select 1 from pg_constraint where conname='playbook_folders_team_fk') then
    alter table playbook_folders add constraint playbook_folders_team_fk foreign key (team_id) references playbook_teams(id);
  end if;
  if not exists (select 1 from pg_constraint where conname='playbook_jobs_team_fk') then
    alter table playbook_jobs    add constraint playbook_jobs_team_fk    foreign key (team_id) references playbook_teams(id);
  end if;
end $$;

-- Indexes for the team_id filter (Wall #1 lookups).
create index if not exists playbook_users_team_idx   on playbook_users   (team_id);
create index if not exists playbook_docs_team_idx     on playbook_docs     (team_id);
create index if not exists playbook_folders_team_idx  on playbook_folders  (team_id);
create index if not exists playbook_jobs_team_idx     on playbook_jobs     (team_id);

-- Email stays GLOBALLY UNIQUE (one email = one account = one team, no team
-- picker at login). The existing unique constraint on playbook_users.email is
-- already exactly what we want — nothing to change.

-- The per-team folder_path unique constraint: folder_path is currently globally
-- unique on playbook_folders. Two teams could legitimately have the same folder
-- name, so this must become UNIQUE PER TEAM. Swap the constraint.
do $$ begin
  if exists (select 1 from pg_constraint where conname='playbook_folders_folder_path_key') then
    alter table playbook_folders drop constraint playbook_folders_folder_path_key;
  end if;
  if not exists (select 1 from pg_constraint where conname='playbook_folders_team_path_key') then
    alter table playbook_folders add constraint playbook_folders_team_path_key unique (team_id, folder_path);
  end if;
end $$;

-- ----------------------------------------------------------------------------
-- 4. Team Admin role flag (independent of coach detection; all default false)
-- ----------------------------------------------------------------------------
alter table playbook_users add column if not exists is_admin boolean not null default false;

-- OPTIONAL SEED (uncomment + set the correct email once you know it): make one
-- Air Force user a Team Admin so AF roster management keeps working after the
-- server step moves roster off coaches. Leave commented for now.
-- update playbook_users set is_admin = true
--   where team_id = (select id from playbook_teams where slug='airforce')
--     and lower(email) = lower('SET_THE_ADMIN_EMAIL_HERE');

commit;

-- ============================================================================
-- WALL #2 (Row-Level Security) — NOT enabled here on purpose. HEADS-UP:
-- the server connects with the Supabase SERVICE ROLE, which BYPASSES RLS by
-- design. So team-scoped RLS policies would be INERT (false confidence) until
-- the server step decides the enforcement mechanism (e.g. run player/coach
-- reads under a non-service role + SET LOCAL app.current_team, or a scoped JWT).
-- Wall #1 (app filters every query by the token's team_id) ships first in the
-- server step; Wall #2 RLS is wired in that same step once the role mechanism
-- is chosen. Documented in docs/BINDER_MULTITENANCY_PLAN.md §4d/§5.
-- ============================================================================

-- Verify after running:
--   select slug, name from playbook_teams;
--   select count(*) filter (where team_id is null) as null_users from playbook_users;   -- expect 0
--   select count(*) filter (where team_id is null) as null_docs  from playbook_docs;    -- expect 0
--   select count(*) filter (where team_id is null) as null_dirs  from playbook_folders; -- expect 0
