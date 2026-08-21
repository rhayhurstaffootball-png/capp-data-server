-- Self-service seat release (Aug 21 2026)
--
-- Schools asked to move CAPP between computers without emailing Roger. The
-- unbind capability already existed (admin reset-seat); what was missing was
-- letting the school do it themselves, at cappvcs.com/seats.
--
-- These columns exist ONLY to rate-limit that. Without a limit, self-release
-- becomes a way to rotate one licence around an entire staff, which is exactly
-- the sharing the seat binding prevents. One release per seat per 24h keeps it
-- no more abusable than emailing support, while still being instant.
--
-- Safe to re-run.

ALTER TABLE capp_clients
    ADD COLUMN IF NOT EXISTS seat_1_released_at timestamptz,
    ADD COLUMN IF NOT EXISTS seat_2_released_at timestamptz;

COMMENT ON COLUMN capp_clients.seat_1_released_at IS
    'Last self-service release of seat 1 (cappvcs.com/seats). Rate limit only; admin resets do not set it.';
COMMENT ON COLUMN capp_clients.seat_2_released_at IS
    'Last self-service release of seat 2 (cappvcs.com/seats). Rate limit only; admin resets do not set it.';
