-- Password reset — run once in the Supabase SQL editor (Jul 17 2026).
-- Adds an email address + reset-token storage to CAPP accounts.
-- Registration only STARTED saving emails when this shipped, so existing
-- accounts have email = null until backfilled via the admin panel.
alter table capp_clients add column if not exists email text;
alter table capp_clients add column if not exists reset_token_hash text;
alter table capp_clients add column if not exists reset_token_expires timestamptz;
