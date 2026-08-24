-- Per-client seat limit + a third seat.
--
-- WHY: seats were hard-wired at two, so a school needing a third had no path
-- except is_admin, which switches machine binding OFF entirely. That trades a
-- licensing model for a support ticket. This keeps binding and makes the COUNT
-- the thing that varies.
--
-- Nebraska is the first case (3 seats). Everyone else stays at 2 by default,
-- so this migration changes nothing for any existing account.
--
-- ⚠ RUN THIS BEFORE DEPLOYING THE SERVER. The new login path reads seat_limit
-- and seat_3_machine; deploying first would 500 every activation.
--
-- ⚠ The Supabase SQL editor splits on ';' WITHOUT respecting quotes, so a
-- semicolon inside a COMMENT string truncates the statement mid-quote and the
-- migration fails. There are deliberately no COMMENT statements here.

ALTER TABLE capp_clients
    ADD COLUMN IF NOT EXISTS seat_3_machine     TEXT,
    ADD COLUMN IF NOT EXISTS seat_3_released_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS seat_limit         INTEGER NOT NULL DEFAULT 2;

-- Belt and braces: any row created before the DEFAULT existed.
UPDATE capp_clients SET seat_limit = 2 WHERE seat_limit IS NULL;

-- A limit outside 1..3 would silently mean "no seats" or "more seats than
-- there are columns", both of which present as a customer locked out.
ALTER TABLE capp_clients
    DROP CONSTRAINT IF EXISTS capp_clients_seat_limit_range;
ALTER TABLE capp_clients
    ADD CONSTRAINT capp_clients_seat_limit_range CHECK (seat_limit BETWEEN 1 AND 3);

-- The reason this migration exists. Adjust the username if it differs.
UPDATE capp_clients SET seat_limit = 3 WHERE username = 'Nebraska1';

-- Verify before deploying: every row should read 2 except Nebraska at 3.
-- SELECT username, seat_limit, seat_1_machine, seat_2_machine, seat_3_machine
-- FROM capp_clients ORDER BY username;
