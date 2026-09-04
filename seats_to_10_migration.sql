-- Raise the per-account seat ceiling from 3 to 10.
--
-- Roger, Sep 4 2026: "Can you change the admin so I can Give up to 10 Licenses".
--
-- ⚠ NOTHING ABOUT MACHINE BINDING CHANGES. Each seat is still bound to one
-- machine and still rate-limited on release. Only the NUMBER of seats moves.
--
-- ⚠ NO CLIENT BUILD IS NEEDED. /auth/login already treats the client's `seat`
-- as a HINT and binds the lowest FREE seat server-side (see the comment in
-- main.py), so an installed 2.6.x/2.7.x asking for "seat_1" on a seventh
-- computer is simply bound to seat 7 and never knows the difference.
--
-- Safe to run more than once: every statement is IF NOT EXISTS.

ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_4_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_5_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_6_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_7_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_8_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_9_machine  text;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_10_machine text;

-- Release timestamps drive the 24h anti-rotation limit. A seat column without
-- its matching released_at would let that seat be recycled without limit.
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_4_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_5_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_6_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_7_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_8_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_9_released_at  timestamptz;
ALTER TABLE capp_clients ADD COLUMN IF NOT EXISTS seat_10_released_at timestamptz;

-- Verify: every account keeps its current seat_limit; nothing is re-bound.
-- select username, seat_limit,
--        seat_1_machine, seat_2_machine, seat_3_machine, seat_4_machine,
--        seat_5_machine, seat_6_machine, seat_7_machine, seat_8_machine,
--        seat_9_machine, seat_10_machine
--   from capp_clients order by username;
