-- CAPP Binder — "Insert Play" support.
-- Lets a coach splice a single new play INTO an existing section booklet at a
-- chosen page (stamped e.g. "8-1") without re-converting/renumbering the rest.
-- Reuses the existing conversion queue (playbook_jobs) so the same worker and
-- "Conversion activity" feed handle inserts too.
-- Run once in the Supabase SQL editor. Additive + idempotent — safe on the
-- live DB; existing convert jobs are unaffected (kind defaults to 'convert').

alter table playbook_jobs add column if not exists kind          text default 'convert';  -- convert | insert
alter table playbook_jobs add column if not exists target_doc_id uuid;    -- section (playbook_docs) an insert job splices into
alter table playbook_jobs add column if not exists insert_after  int;     -- splice after this physical page (0 = before page 1)
alter table playbook_jobs add column if not exists label         text;    -- number to stamp on the inserted page(s), e.g. "8-1"

-- Shift a doc's Touch Notes up by p_shift for every page past p_after, so a
-- player's note stays stuck to the SAME play after pages are inserted above it.
-- Done in two steps through a large offset so the unique key (email,doc_id,page)
-- can never transiently collide mid-update (a plain page = page + K would hit a
-- row that hasn't moved yet when two adjacent pages both have a note). Service
-- role only (playbook_notes has RLS; SECURITY DEFINER lets the worker path run
-- it via the server).
create or replace function shift_playbook_notes(p_doc_id uuid, p_after int, p_shift int)
returns void
language plpgsql
security definer
as $$
begin
  if coalesce(p_shift, 0) = 0 then
    return;
  end if;
  update playbook_notes set page = page + 1000000
   where doc_id = p_doc_id and page > p_after;
  update playbook_notes set page = page - 1000000 + p_shift
   where doc_id = p_doc_id and page > 1000000;
end;
$$;
