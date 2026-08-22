-- In-app broadcast notices (Aug 21 2026)
--
-- Roger: "I also need a way to Blast all users with update info."
--
-- Today updates are pull-only at launch: the client checks /app/version and
-- offers an update in a dialog that auto-dismisses to "Later". Nothing reaches
-- a client that is already running, and there is no way to say anything at all
-- ("server maintenance Saturday", "new feature", "update is mandatory").
--
-- ⚠ This lives in a TABLE, not a Render env var, on purpose. APP_VERSION is an
-- env var and it has already bitten us: it was set moments after a deploy
-- started and /agent/version served the stale value until the next deploy.
-- A broadcast you can only send by redeploying is not a broadcast.
--
-- ⚠ Keep semicolons OUT of any quoted string here -- the Supabase SQL editor
-- splits statements on ';' without respecting quotes and fails with
-- "ERROR: 42601: unterminated quoted string".
--
-- Safe to re-run.

CREATE TABLE IF NOT EXISTS capp_notices (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    title       text NOT NULL,
    body        text NOT NULL,
    severity    text NOT NULL DEFAULT 'info',   -- info | warning | critical
    -- Clients below this version are blocked with a non-dismissible prompt.
    -- Empty means the notice is informational and the update stays optional.
    min_version text,
    active      boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS capp_notices_active_idx
    ON capp_notices (active, created_at DESC);

ALTER TABLE capp_notices ENABLE ROW LEVEL SECURITY;

-- Server reaches this with the service key, which bypasses RLS. No policy is
-- granted to anon or authenticated on purpose -- notices are published by the
-- admin panel and read through the API, never straight from a client.
