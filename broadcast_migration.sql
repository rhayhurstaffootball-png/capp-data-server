-- Message blasts to customer schools.
--
-- This table is the EMAIL side and the send history. It is NOT a second notice
-- system.
--
-- ⚠ An in-app notice system ALREADY EXISTS: `capp_notices`, read by
-- /app/notice, built Aug 21 2026. Ticking "also show in the app" on a blast
-- publishes a row THERE and records its id here as notice_id. Do not add
-- message text to this table for the app to read - two copies where only one
-- gets updated is a pattern that has already caused real bugs in CAPP.
--
-- What is genuinely new here is EMAIL: reaching schools whether or not CAPP is
-- open, and against every installed build with nothing to update.
--
-- ⚠ The Supabase SQL editor splits on ';' WITHOUT respecting quotes, so a
-- semicolon inside a COMMENT string truncates it mid-quote. No COMMENTs here.

CREATE TABLE IF NOT EXISTS capp_broadcasts (
    id            BIGSERIAL PRIMARY KEY,
    subject       TEXT        NOT NULL,
    body          TEXT        NOT NULL,

    -- 'licensed' | 'licensed_trial' | 'all'
    audience      TEXT        NOT NULL DEFAULT 'licensed_trial',

    -- Whether this went out by email, and whether it should also show in the
    -- app. Kept separate: most messages are one or the other, not both.
    send_email    BOOLEAN     NOT NULL DEFAULT TRUE,
    show_in_app   BOOLEAN     NOT NULL DEFAULT FALSE,

    active        BOOLEAN     NOT NULL DEFAULT TRUE,

    -- When a blast is also shown in the app, the in-app copy lives in
    -- capp_notices (which already existed and is what /app/notice reads).
    -- This points at it. There is deliberately NO second copy of the message
    -- text driving the app - one message, one source of truth.
    notice_id     BIGINT,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at       TIMESTAMPTZ,
    sent_count    INTEGER     NOT NULL DEFAULT 0,
    failed_count  INTEGER     NOT NULL DEFAULT 0,

    -- Exactly who it reached, so "did Nebraska get it?" is answerable.
    recipients    JSONB       NOT NULL DEFAULT '[]'::jsonb,

    created_by    TEXT
);

CREATE INDEX IF NOT EXISTS capp_broadcasts_created_idx
    ON capp_broadcasts (created_at DESC);



-- Verify:
-- SELECT id, subject, audience, sent_at, sent_count FROM capp_broadcasts
-- ORDER BY created_at DESC LIMIT 10;
