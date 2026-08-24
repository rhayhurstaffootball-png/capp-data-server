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
-- ⚠ The Supabase SQL editor splits on ';' without respecting quotes, so there
-- are no COMMENT statements here.

ALTER TABLE playbook_jobs
    ADD COLUMN IF NOT EXISTS review BOOLEAN NOT NULL DEFAULT FALSE;

-- Existing rows keep the old behaviour (publish straight away).
UPDATE playbook_jobs SET review = FALSE WHERE review IS NULL;

-- Verify:
-- SELECT id, title, ext, status, review FROM playbook_jobs
-- ORDER BY created_at DESC LIMIT 10;
