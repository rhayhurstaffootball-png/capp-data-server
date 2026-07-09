-- CAPP Binder — universal page numbering for coach uploads.
-- Adds a per-job flag so the worker stamps booklet page numbers (1..N,
-- bottom-center, same style as the Visio Converter) onto a coach's upload,
-- so the whole playbook stays consistently numbered no matter who added a
-- section. Default false so EXISTING behavior (admin uploads, old jobs) is
-- unchanged — only the coach endpoints set it true.
-- Run once in the Supabase SQL editor. Additive + idempotent.

alter table playbook_jobs add column if not exists number boolean default false;
