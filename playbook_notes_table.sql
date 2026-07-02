-- Touch Notes: private, per-player, per-play (doc page) typed notes.
-- Run once in the Supabase SQL editor.
create table if not exists playbook_notes (
  id         uuid primary key default gen_random_uuid(),
  email      text        not null,
  doc_id     uuid        not null,
  page       int         not null,
  body       text        not null default '',
  updated_at timestamptz not null default now()
);

-- One note per player per play → the upsert target.
create unique index if not exists playbook_notes_uniq
  on playbook_notes (email, doc_id, page);

-- Fast "all of my notes" fetch.
create index if not exists playbook_notes_email
  on playbook_notes (email);
