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
- Live and finished games (a coach who pulls after the final gets the same fill). Measured on 125 finished games: 0 rows.
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
PRIMARY_NOTE = "Backup source"                     # once the backup IS the feed, the row is normal, not a warning
WHY = "CBS has this play, ESPN has not published it yet"
RECENT = 8                                          # ESPN rows of the last quarter checked for "same play"


def _q(v):
    """Quarter as a number. ESPN labels overtime "OT" / "2OT" (CBS numbers it 5, 6, ...): MEASURED Sep 19 2026 on the
    Sep 12 corpus - reading "OT" as 0 made the whole game "after" ESPN's last play on both overtime games."""
    t = str(v or "").strip().upper()
    if t.isdigit():
        return int(t)
    if t.endswith("OT"):
        n = t[:-2]
        return 4 + (int(n) if n.isdigit() else 1)
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


STATUSES = ("in", "post")   # live AND finished games. MEASURED Sep 19 2026 on the Sep 12 corpus (125 finished games,
                            # ESPN complete): 0 rows appended, 0 crashes - once "OT" parsed as quarter 5 (see _q).

# ── HAS THE BACKUP BECOME THE PRIMARY? ───────────────────────────────────────────────────────────────────────────
# Roger, Sep 19 2026 night: "that Red is scary as fuck... make it White and they get an alert 'Now using backup
# source' and have it just pick up." And the limit he set on it: a single CBS play dropped into ESPN's flow, or a
# tail row or two while ESPN is merely a play behind, STAYS RED - it is a play the coach should look at. White is
# only for the case that actually happened on Saturday: ESPN quiet and CBS carrying the game.
# Roger, Sep 20 2026, asked directly: rows ALWAYS appear the moment CBS is ahead - this decides colour and the
# one-time alert, never whether the coach sees a play.
# The two shapes, from Saturday's own numbers: ESPN dark on Nebraska gave a tail of 64 rows (MTSU 79, MD 34,
# BYU 19); a lone missing play gives 1 or 2. Either signal is enough on its own.
PRIMARY_TAIL_ROWS = 3        # CBS carrying this many plays past ESPN's last published one
PRIMARY_QUIET_SECS = 90      # ...or ESPN has published nothing at all for this long


def _wallclock_secs(entries, now=None):
    """Seconds since the newest ESPN play was published, or None when no row carries a usable wallclock."""
    import calendar
    import time as _t
    newest = None
    for e in entries:
        if e.get("cbs_tail"):
            continue
        w = str(e.get("wallclock") or "").strip()
        if not w.endswith("Z") or len(w) < 20:
            continue
        try:
            t = calendar.timegm(_t.strptime(w, "%Y-%m-%dT%H:%M:%SZ"))
        except ValueError:
            continue
        if newest is None or t > newest:
            newest = t
    if newest is None:
        return None
    return max(0.0, (now if now is not None else _t.time()) - newest)


def backup_is_primary(entries, appended, status="in", now=None):
    """True when CBS has taken the game over, not merely filled a hole. Finished games never count: a post-game
    pull is a backfill, nobody is watching a feed."""
    if str(status or "") != "in" or appended <= 0:
        return False
    if appended >= PRIMARY_TAIL_ROWS:
        return True
    quiet = _wallclock_secs(entries, now)
    return quiet is not None and quiet >= PRIMARY_QUIET_SECS


def append_tail(entries, doc, home_name, away_name, status="in"):
    """Append CBS's tail rows to `entries` in place.

    Returns (rows_appended, backup_is_primary). The second value is what tells the coach app to paint the rows
    white and raise its one-time "Now using backup source" alert instead of the red every added row gets.
    """
    if str(status or "") not in STATUSES or not doc or not doc.get("available"):
        return 0, False
    tail = tail_items(entries, doc.get("items") or [])
    if not tail:
        return 0, False
    sub = {"items": tail}
    rows = []
    for q in sorted({_q(it.get("quarter")) for it in tail}):
        if 1 <= q <= 5:
            rows.extend(cbs_rows.rows_for_quarter(sub, q, home_name, away_name))
    primary = backup_is_primary(entries, len(rows), status)
    for r in rows:
        r.update(wallclock="", espn_play_id="", espn_seq=None, _forced_key=_key(r), cbs_tail=True,
                 ncaa_status="added", qc_issue=(PRIMARY_NOTE if primary else NOTE),
                 backup_primary=primary,
                 ncaa_changes=[{"field": "row", "old": "", "new": "added", "why": WHY}])
    entries.extend(rows)
    return len(rows), primary
