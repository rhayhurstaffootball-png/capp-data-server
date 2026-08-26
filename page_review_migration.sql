-- Page selection ("review") for converted uploads.
--
-- A coach drops a PowerPoint / Excel / Visio file, it converts as usual, and
-- instead of publishing straight away the job stops in `review`. The coach
-- picks which pages they actually want, and only those are published.
-- Unselected pages are discarded.
--
-- ⚠ NUMBERING: a review job is sent to the local converter with number=false,
-- and the SERVER stamps 1..N after the selection. If the converter numbered
-- first, extracting pages 3, 7 and 9 would leave a document visibly numbered
-- "3, 7, 9". Doing it after selection is also why this needs NO converter
-- rebuild - the converter already honours number=false.
--
-- ⚠ The Supabase SQL editor splits on the semicolon character without
-- respecting quotes, so there are no COMMENT statements here -- and no
-- semicolon appears anywhere in this file except as a real statement
-- terminator, INCLUDING inside comments. A semicolon inside a quoted string
-- (even a commented-out one) splits mid-quote and the paste dies with
-- "ERROR: 42601: unterminated quoted string". This file previously described
-- that rule using a quoted semicolon, which tripped the very trap it warned
-- about and is the likeliest reason this migration never got run.

ALTER TABLE playbook_jobs
    ADD COLUMN IF NOT EXISTS review BOOLEAN NOT NULL DEFAULT FALSE;

-- Existing rows keep the old behaviour (publish straight away).
UPDATE playbook_jobs SET review = FALSE WHERE review IS NULL;

-- ⚠ number_after WAS MISSING FROM THIS FILE and is NOT optional. Job creation
-- inserts it on EVERY upload (main.py, coach_pb_create_job), so without it the
-- insert is rejected with PGRST204 and NOTHING can be uploaded to the Binder -
-- not Word, Excel, PowerPoint, Visio, nor even a PDF. Adding only `review`
-- leaves uploads just as broken. Found live Aug 26 2026, with the page-
-- selection code already deployed to Render and both columns absent.
--
-- Defaults TRUE because both the reader (job.get("number_after", True)) and
-- the writer (payload.get("number", True)) default to numbering pages, so
-- TRUE is what preserves existing behaviour for rows created before this ran.
ALTER TABLE playbook_jobs
    ADD COLUMN IF NOT EXISTS number_after BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE playbook_jobs SET number_after = TRUE WHERE number_after IS NULL;

-- Verify:
-- SELECT id, title, ext, status, review FROM playbook_jobs
-- ORDER BY created_at DESC LIMIT 10;
