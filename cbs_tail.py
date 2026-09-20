"""CBS TAKES OVER THE TAIL of a live game when ESPN's play feed is behind (Roger, Sep 19 2026).

Sat Sep 19, the 5 PM block: ESPN's play-by-play sat 5-7 minutes of game clock behind CBS on Maryland and MTSU and was
BLANK on Nebraska (0 drives) while ESPN's own scoreboard clock kept moving - three licensed coaches with nothing coming
in. Roger: "I HAVE to get them this data" / "I thought CBS would just take over if this happened". Until now CBS only
verified and fixed ESPN's rows, added single missing plays once ESPN had moved past them, and replaced a FINISHED
quarter on the coach's word - never the live tail.

What this does: every CBS play that sits AFTER the last play ESPN has published (a later quarter, or the same quarter
at a lower clock) is appended to the entries as a row of its own, built by the same converter the coach app's
quarter swap uses (cbs_rows.py = SBENTRY/quarter_swap.py): kickoffs, tries, field goals, team timeouts, possession,
down / distance / spot and CBS's own scores. Each row carries a stable "gap:cbs:tail:..." key and ncaa_status "added",
so the coach app draws it live and, on the poll where ESPN publishes the play itself, the CBS row is no longer in the
tail, its key disappears from the feed, and the app takes it down the way it takes down every server-made gap row
(espn_poller._drop_gone_placeholders) - ESPN's own version is drawn in its place.

Guards:
- Live games only (ESPN status "in"). A finished game's tail is a different question (never measured) - not here.
- A CBS play that is ESPN's LAST play written differently (same play / same snap, cbs_check's tests against the last
  rows of that quarter) is not appended - the tail starts strictly after what ESPN has.
- The same clock as ESPN's last play is NOT "after" (a tempo snap on the same second will come from ESPN a poll later).
- Rows are marked cbs_tail=True: espn_fetcher does not count them toward a quarter's issue share and the score
  passes have already run when they go in (they keep CBS's scores).
"""
import hashlib

import cbs_check
import cbs_rows

NOTE = "Backup source - primary feed behind"       # Roger, Sep 13 2026: no vendor names on screen
WHY = "CBS has this play, ESPN has not published it yet"
RECENT = 8                                          # ESPN rows of the last quarter checked for "same play"


def _q(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 0


def _secs(clock):
    try:
        m, s = str(clock).strip().split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def tail_items(entries, items):
    """The CBS items that come after ESPN's last published play. Pure; tested on its own."""
    items = sorted(items or [], key=lambda it: it.get("seq", 0))
    real = [e for e in entries if not e.get("cbs_tail")]
    if real:
        last = real[-1]
        lq, ls = _q(last.get("quarter")), _secs(last.get("clock"))
        recent = [e for e in real[-RECENT:] if _q(e.get("quarter")) == lq]
    else:
        lq, ls, recent = 0, None, []

    def after(it):
        q = _q(it.get("quarter"))
        if q > lq:
            return True
        if q < lq or ls is None:
            return False
        s = it.get("clock_secs")
        return s is not None and s < ls

    out = []
    for it in items:
        if not after(it):
            continue
        if it.get("kind") == "play" and any(cbs_check.same_play(e, it) or cbs_check.same_snap(e, it) for e in recent):
            continue
        out.append(it)
    return out


def _key(r):
    body = "%s|%s|%s|%s|%s" % (r.get("quarter"), r.get("clock"), r.get("down"), r.get("distance"), r.get("play_text"))
    return "gap:cbs:tail:%s:%s" % (r.get("quarter"), hashlib.sha1(body.encode("utf-8")).hexdigest()[:12])


def append_tail(entries, doc, home_name, away_name, status="in"):
    """Append CBS's tail rows to `entries` in place. Returns the number appended (0 = nothing to do)."""
    if str(status or "") != "in" or not doc or not doc.get("available"):
        return 0
    tail = tail_items(entries, doc.get("items") or [])
    if not tail:
        return 0
    sub = {"items": tail}
    rows = []
    for q in sorted({_q(it.get("quarter")) for it in tail}):
        if 1 <= q <= 5:
            rows.extend(cbs_rows.rows_for_quarter(sub, q, home_name, away_name))
    for r in rows:
        r.update(wallclock="", espn_play_id="", espn_seq=None, _forced_key=_key(r), cbs_tail=True,
                 ncaa_status="added", qc_issue=NOTE,
                 ncaa_changes=[{"field": "row", "old": "", "new": "added", "why": WHY}])
    entries.extend(rows)
    return len(rows)
