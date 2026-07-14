-- ============================================================================
-- Enable Row-Level Security on EVERY public table — fixes the Supabase
-- security-advisor emails ("rls_disabled_in_public" + "sensitive_columns_exposed",
-- 12 Jul 2026). Written Jul 14 2026.
--
-- WHY THIS IS SAFE TO RUN (verified against the code Jul 14 2026):
--   * Every Supabase call in main.py, pb_worker.py, create_client.py,
--     db_updater.py, build_merged_db.py and the dev tools uses the
--     SERVICE-ROLE key, which has BYPASSRLS — RLS never applies to it.
--   * The ONLY exception is the Wall #2 scoped-JWT path (_scoped_headers,
--     anon key + team_id JWT), which touches exactly five tables:
--     playbook_docs / playbook_folders / playbook_jobs / playbook_users
--     (all four already have team_isolation policies from
--     playbook_rls_policies.sql, run + verified live Jul 8 2026) and ONE
--     read of playbook_push_subscriptions (main.py notify flow) — that
--     table gets its missing SELECT policy below, BEFORE RLS is enabled.
--   * No client ever talks to Supabase directly: the desktop suite, the
--     Binder web app/PWA, and the local converter EXE all go through the
--     Render server (or the DO relay). The anon key is not shipped anywhere.
--
-- EFFECT: anyone holding only the project URL + anon key gets zero rows /
-- zero writes on every table. Server behavior is unchanged.
--
-- Run the whole file once in the Supabase SQL editor. Idempotent — safe to
-- re-run (ENABLE RLS on an already-enabled table is a no-op; policies are
-- drop-and-recreate).
-- ============================================================================

-- 1. The one missing policy first, so the scoped notify-flow read keeps
--    working the instant RLS turns on for playbook_push_subscriptions.
--    (Subscribe/unsubscribe/delete on this table use the service key.)
drop policy if exists team_isolation_select on playbook_push_subscriptions;
create policy team_isolation_select on playbook_push_subscriptions
  for select
  using (team_id = ((auth.jwt() ->> 'team_id'))::uuid);

-- 2. Enable RLS on every table in the public schema — including any table
--    not named in code (covers whatever the advisor flagged, present and
--    future rows alike). Tables that already have RLS (capp_prospects,
--    the four playbook Wall #2 tables) are unaffected no-ops.
do $$
declare t record;
begin
  for t in select tablename from pg_tables where schemaname = 'public'
  loop
    execute format('alter table public.%I enable row level security', t.tablename);
  end loop;
end $$;

-- ============================================================================
-- VERIFY (run after):
--   1. Every table shows rowsecurity = true:
--        select tablename, rowsecurity from pg_tables
--          where schemaname = 'public' order by tablename;
--   2. Anon key gets nothing (repeat the Jul 8 raw-REST test against
--      capp_clients and messages — expect [] / 401-empty, never data).
--   3. Supabase dashboard → Advisors → Security Advisor → refresh: the
--      rls_disabled_in_public and sensitive_columns_exposed criticals clear.
--   4. App smoke test: Binder login + manifest + a push-notified upload
--      (exercises the scoped playbook_push_subscriptions read), CAPP Friends
--      send/receive, admin panel client list + CRM tab.
-- ============================================================================
