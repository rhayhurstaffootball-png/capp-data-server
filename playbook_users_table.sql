-- Playbook Portal — player accounts.
-- Admin uploads the roster (first/last/position/email); each player sets their
-- own password on first login. Email is the login + the allowlist.
-- Run once in the Supabase SQL editor.

create table if not exists playbook_users (
  id              uuid primary key default gen_random_uuid(),
  email           text unique not null,
  first_name      text default '',
  last_name       text default '',
  position        text default '',
  pw_salt         text,                       -- set on first login
  pw_hash         text,                       -- set on first login (sha256+salt)
  password_set_at timestamptz,
  created_at      timestamptz default now()
);

-- Server talks to this table with the service key (bypasses RLS). Enable RLS with
-- no policies so nothing else (e.g. the anon key) can read password material.
alter table playbook_users enable row level security;


Tori 
