-- Playbook Portal — content (folders + PDFs).
-- Folder tree metadata; the PDF bytes live in Cloudflare R2 (r2_key).
-- Run once in the Supabase SQL editor.

create table if not exists playbook_docs (
  id          uuid primary key default gen_random_uuid(),
  folder_path text default '',        -- e.g. "04 CALLS/02 PRESSURES"  ('' = root)
  title       text not null,          -- display name (the leaf), e.g. "01 PLUTO"
  r2_key      text not null,          -- object key in the R2 bucket
  pages       int,
  size_bytes  bigint,
  sort_order  int default 0,          -- ordering within a folder
  created_at  timestamptz default now()
);

create index if not exists playbook_docs_folder_idx on playbook_docs (folder_path);

-- Server (service key) is the only writer/reader; enable RLS with no policies.
alter table playbook_docs enable row level security;
