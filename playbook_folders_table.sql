-- Playbook Portal — empty-folder support.
-- A row here makes a folder visible in the portal tree even when no PDF lives
-- in it yet (the tree is otherwise built only from playbook_docs.folder_path).
-- Used for pre-created structure like the season's game-plan folders.
-- Run once in the Supabase SQL editor.

create table if not exists playbook_folders (
  id          uuid primary key default gen_random_uuid(),
  folder_path text not null unique,    -- e.g. "2026 GAME PLANS/01 DUQUESNE (SEP 5)"
  created_at  timestamptz default now()
);

-- Server (service key) is the only writer/reader; enable RLS with no policies.
alter table playbook_folders enable row level security;
