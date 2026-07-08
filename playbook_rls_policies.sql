-- ============================================================================
-- CAPP Binder — WALL #2: Row-Level Security policies (Step 2b)
-- Written Jul 8 2026.  Plan: docs/BINDER_MULTITENANCY_PLAN.md §4d/§5
--
-- WHAT THIS DOES: adds RLS policies to playbook_docs, playbook_folders,
-- playbook_jobs, playbook_users that check the request's JWT team_id claim
-- against each row's team_id column. This is the DATABASE'S OWN lock —
-- independent of and in addition to the app-level team_id filters already
-- shipped in main.py (Wall #1). Even if application code ever forgets a
-- filter, the database itself will refuse to return or accept a row for
-- the wrong team.
--
-- HOW IT'S ENFORCED: main.py's _scoped_headers(team_id) mints a short-lived
-- JWT with {role: "authenticated", team_id: "<uuid>"} signed with the
-- project's JWT secret, and uses it (+ the anon key) instead of the
-- service-role key for every player/coach/team-admin database call. Postgres
-- RLS policies below read that claim via auth.jwt()->>'team_id'.
--
-- ⚠ THIS FILE HAS NO EFFECT ON ITS OWN — it depends on TWO Render env vars
-- being set on capp-data-server:
--     SUPABASE_JWT_SECRET   (Supabase dashboard → Project Settings → API →
--                             "JWT Secret", under "JWT Settings")
--     SUPABASE_ANON_KEY     (same page → Project API keys → "anon public")
-- Until BOTH are set, main.py's _scoped_headers() silently falls back to the
-- service-role key (today's Wall #1-only behavior) — so it is SAFE to run
-- this SQL at any time; it only starts actually blocking cross-team access
-- once those two env vars land on Render AND this SQL has run. Recommended
-- order: run this SQL first (inert until the env vars exist), THEN set the
-- env vars, THEN run the dummy-second-team acceptance test.
--
-- The service_role key (Owner admin panel + the pb_worker.py conversion
-- worker) ALWAYS bypasses RLS by Postgres/Supabase design (service_role has
-- the BYPASSRLS attribute) — that is intentional: the Owner is meant to see
-- across all teams, and the worker is a trusted internal batch process, not
-- reachable by any team's coach or player.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- playbook_docs — players/coaches read+write only their own team's docs
-- ----------------------------------------------------------------------------
drop policy if exists team_isolation_select on playbook_docs;
create policy team_isolation_select on playbook_docs
  for select
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_insert on playbook_docs;
create policy team_isolation_insert on playbook_docs
  for insert
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_update on playbook_docs;
create policy team_isolation_update on playbook_docs
  for update
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid)
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_delete on playbook_docs;
create policy team_isolation_delete on playbook_docs
  for delete
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

-- ----------------------------------------------------------------------------
-- playbook_folders — same shape
-- ----------------------------------------------------------------------------
drop policy if exists team_isolation_select on playbook_folders;
create policy team_isolation_select on playbook_folders
  for select
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_insert on playbook_folders;
create policy team_isolation_insert on playbook_folders
  for insert
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_update on playbook_folders;
create policy team_isolation_update on playbook_folders
  for update
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid)
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_delete on playbook_folders;
create policy team_isolation_delete on playbook_folders
  for delete
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

-- ----------------------------------------------------------------------------
-- playbook_jobs — conversion queue, same shape
-- ----------------------------------------------------------------------------
drop policy if exists team_isolation_select on playbook_jobs;
create policy team_isolation_select on playbook_jobs
  for select
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_insert on playbook_jobs;
create policy team_isolation_insert on playbook_jobs
  for insert
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_update on playbook_jobs;
create policy team_isolation_update on playbook_jobs
  for update
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid)
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_delete on playbook_jobs;
create policy team_isolation_delete on playbook_jobs
  for delete
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

-- ----------------------------------------------------------------------------
-- playbook_users (roster) — Team Admin roster tools read+write only their
-- own team's rows. NOTE: pre-login lookups (playbook_check/setup/login,
-- which don't know the caller's team yet — they look up ANY email to find
-- out which team it belongs to) intentionally stay on the SERVICE-ROLE key
-- in main.py, not the scoped JWT, and therefore correctly bypass these
-- policies. That is required for login to work at all and does not leak
-- playbook CONTENT — those endpoints only ever return {status, first_name}.
-- ----------------------------------------------------------------------------
drop policy if exists team_isolation_select on playbook_users;
create policy team_isolation_select on playbook_users
  for select
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_insert on playbook_users;
create policy team_isolation_insert on playbook_users
  for insert
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_update on playbook_users;
create policy team_isolation_update on playbook_users
  for update
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid)
  with check (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

drop policy if exists team_isolation_delete on playbook_users;
create policy team_isolation_delete on playbook_users
  for delete
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

-- ============================================================================
-- VERIFY (run these after the two Render env vars are set):
--   1. Log in as a Team B player/coach and confirm /playbook/manifest,
--      /coach/playbook/folders, /team-admin/roster all come back EMPTY of
--      Team A's data (not just filtered client-side — actually empty from
--      the DB, provable by temporarily removing a team_id filter in a test
--      branch and confirming RLS still returns nothing for the wrong team).
--   2. select tablename, policyname, roles, cmd from pg_policies
--        where tablename in ('playbook_docs','playbook_folders',
--                             'playbook_jobs','playbook_users')
--        order by tablename, cmd;
--      -> should show 4 policies (select/insert/update/delete) per table.
-- ============================================================================
