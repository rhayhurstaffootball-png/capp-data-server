-- Converter version tracking (Aug 9 2026)
--
-- Each paired converter reports the build it's running on every check-in
-- (x-converter-version header -> _worker_identity). This column stores it so
-- the Binder can tell a coach their converter is stale.
--
-- Safe to run before or after the server deploy: the server writes the column
-- if it exists and falls back to a bare heartbeat if it doesn't, so paired
-- converters never appear offline in between.
--
-- Run in the Supabase SQL editor.

ALTER TABLE playbook_converter_devices
    ADD COLUMN IF NOT EXISTS converter_version text;

-- A machine with NULL here is running a build from before version reporting
-- existed. Those cannot self-update either, so they need a manual reinstall —
-- this query is the list of who to chase.
SELECT email,
       device_name,
       COALESCE(converter_version, '(pre-update build — reinstall needed)') AS running,
       last_seen_at
FROM playbook_converter_devices
ORDER BY last_seen_at DESC NULLS LAST;
