-- CRM prospects table for the admin panel "CRM" tab.
-- Run this once in Supabase → SQL Editor → New query → Run.
-- Separate from capp_clients (licensed customers); this tracks demo prospects.

create table if not exists public.capp_prospects (
    id              uuid primary key default gen_random_uuid(),
    school          text not null,
    contact         text,
    email           text,
    phone           text,
    status          text not null default 'Demo Done',
    quote_sent_date date,
    notes           text,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

-- Lock it down to the server's service-role key only (which bypasses RLS).
-- With RLS enabled and no policies, the public/anon key cannot read or write.
alter table public.capp_prospects enable row level security;

-- Helpful index for the "most recently updated first" listing.
create index if not exists capp_prospects_updated_idx
    on public.capp_prospects (updated_at desc);
