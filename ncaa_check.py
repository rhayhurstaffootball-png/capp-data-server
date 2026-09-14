"""NCAA check - ESPN is the play data, NCAA verifies it.

⚠ Roger, Sep 13 2026: "The NCAA is the Verifier, we take the play Data from ESPN and Compare it to make sure its
accurate." ... "When a Play doesnt match we Auto Fix but Show them 'This many Plays need Reviewed'".

ESPN's rows ARE the play list. Each one is lined up with NCAA's play for it (ncaa_match._align_quarter - order-aware,
content-scored, one quarter at a time) and gets `ncaa_status`:
  verified    NCAA has the same play and agrees
  fixed       NCAA has the same play and disagrees on down / distance / spot, or has the real clock where ours is only
              a best guess - NCAA's value applied, every change recorded old -> new in `ncaa_changes` for review/undo
  unverified  NCAA has no counterpart - kept EXACTLY as ESPN sent it, never deleted. Normal: NCAA replaces some
              kickoffs / first snaps with "drive start" lines, and runs behind a live game
  added       a play NCAA has and ESPN does not, added in place and flagged for review

MEASURED on Sep 12 2026's five games before building (_dev_tools/game_replay_test/analyze_ncaa_fields.py, graded by
the hand-fixed game files - which are never a source): ESPN and NCAA agreed on down and spot for 553 of 555 lined-up
plays. Of 8 field disagreements NCAA was right on 6; the other 2 were ONE WRONG PAIRING (a Q2 0:25 rush lined up with
a Q2 10:48 punt) - which is what _good_pair() rejects.
⚠ "NCAA plays ESPN lacks": 6 looked real, but 5 were NCAA listing a play ESPN already has TWICE (see _already_have).
Only 1 was truly missing (Nebraska Q1 5:33) - and that is the only play the check adds on those games.
"""
import difflib
import hashlib
import re
from collections import Counter

import ncaa_match as M

# NCAA's situation line. Every shape seen on Sep 12: "2 and  at NDS31" (no distance, side given),
# "1 and 10 at 35" (no side), "2 and true at AF 31" (junk distance).
_SITU = re.compile(r"^\s*(\d)\s+and\s+(\S*)\s+at\s+([A-Za-z][A-Za-z .&']*?)?\s*(\d{1,2})\s*$", re.I)
_GAIN = re.compile(r"for\s+(\d+)\s+yards?\s+(gain|loss)", re.I)
_DRIVE_START = re.compile(r"drive start at\s+(\d{1,2}:\d{2})", re.I)
_SCORING = re.compile(r"touchdown|field goal|safety|kick attempt|extra point|two-point|2pt", re.I)
_TIMEOUT = re.compile(r"^\s*(\(\d{1,2}:\d{2}\)\s*)?(officials\s+)?time\s*out", re.I)
_NOT_CHECKED_DOWNS = {"KO", "EP", "2PT", "OTO", "FG"}

_MIN_PAIR_SCORE = 7
# NCAA writes "Ridley,Brennan"; ESPN and every board write "B.Ridley".
_NCAA_NAME = re.compile(r"\b([A-Z][A-Za-z'\-]+),([A-Z])[A-Za-z'\-]*")


def situation(drive_text):
    """(down, distance or None, side or None, yard or None) from NCAA's 'D and DIST at SPOT'."""
    m = _SITU.match(str(drive_text or ""))
    if not m:
        return None, None, None, None
    dist = int(m.group(2)) if m.group(2).isdigit() else None
    side = m.group(3).strip().upper() if m.group(3) else None
    return m.group(1), dist, side, int(m.group(4))


def _int(v):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _ncaa_secs(t):
    """NCAA's clock for a play, or None. 0:00 on a mid-quarter play means NCAA had no clock."""
    s = M.clock_secs(M.text_clock(t.get("text"))) or M.clock_secs(t.get("clock"))
    return s if s else None


def _good_pair(entry, t, clock_src):
    """Is NCAA's play really this ESPN play? Content first, then the clocks must be close."""
    if M.score_pair(entry, t) < _MIN_PAIR_SCORE:
        return False
    mine = M.clock_secs(entry.get("clock"))
    theirs = _ncaa_secs(t)
    if mine is not None and theirs is not None:
        limit = 180 if clock_src == "guess" else 60      # a guessed clock can be a minute or more out
        if abs(mine - theirs) > limit:
            return False
    return True


def _espn_names(text):
    return _NCAA_NAME.sub(lambda m: "%s.%s" % (m.group(2), m.group(1)), str(text or ""))


def _plain(text):
    """Play text with the things the two houses write differently taken out: the "(MM:SS)" prefix, jersey
    numbers and NCAA's "Last,First" names."""
    t = _espn_names(text)
    t = re.sub("^ *[(][0-9]{1,2}:[0-9]{2}[)] *", "", t)
    t = re.sub("#[0-9]+ ?", "", t)
    return " ".join(t.lower().split())


def _already_have(entries, q, t):
    """Does ESPN already have this NCAA play somewhere in the quarter?

    ⚠ Sep 13 2026, first build: NCAA lists some plays TWICE (Nebraska Q2 3:11, 1:54, Q4 8:01; Air Force Q1 3:27).
    The aligner pairs ESPN's play with one copy and leaves the other over, which looked exactly like a play ESPN was
    missing - so the check added a second copy of plays ESPN already had. Every one of those carried the same
    "(MM:SS)" snap time as ESPN's play and the same player.
    ⚠ NOT "a strong content match nearby": that also swallowed a genuinely missing play whenever the same player had
    a similar play seconds earlier - back-to-back incompletions by one quarterback (caught by test_ncaa_check.py).
    So: same written snap time AND a shared surname; or, when NCAA wrote no time, near-identical wording within 30s.
    """
    their_tc = M.clock_secs(M.text_clock(t.get("text")))
    their_names = M._surnames(t.get("text"))
    theirs = _ncaa_secs(t)
    their_plain = _plain(t.get("text"))
    for e in entries:
        if M._quarter(e.get("quarter")) != q:
            continue
        txt = e.get("play_text", "")
        if their_tc is not None:
            if M.clock_secs(M.text_clock(txt)) == their_tc and (their_names & M._surnames(txt)):
                return True
            continue
        mine = M.clock_secs(e.get("clock"))
        if (theirs is not None and mine is not None and abs(theirs - mine) <= 30
                and difflib.SequenceMatcher(None, their_plain, _plain(txt)).ratio() >= 0.9):
            return True
        # ⚠ Neither side typed a time, so ESPN's copy can sit anywhere in the quarter: SMU Q2 (Sep 12) ESPN typed the
        # Fisher run late, 76s from where NCAA lists it, and a second copy was added. Same wording AND the very same
        # numbers (yards, spot) - two different plays don't share those. Measured on Sep 12's 5 games: blocks only that.
        if (M.clock_secs(M.text_clock(txt)) is None
                and re.findall(r"\d+", their_plain) == re.findall(r"\d+", _plain(txt))
                and difflib.SequenceMatcher(None, their_plain, _plain(txt)).ratio() >= 0.9):
            return True
    return False


def _added_key(q, text):
    """Stable across polls, and a "gap:" key - so a live client already removes the row once the server stops
    sending it, which is what happens when ESPN delivers the play itself (espn_poller._drop_gone_placeholders)."""
    body = re.sub(r"^\s*\(\d{1,2}:\d{2}\)\s*", "", M._fold(text))
    return "gap:ncaa:%s:%s" % (q, hashlib.sha1(re.sub(r"\s+", " ", body).encode("utf-8")).hexdigest()[:12])


# Line-up results per quarter, keyed by exactly what the matcher reads. A finished quarter never changes during a
# game, so each live update only re-lines-up the quarter being played - and an update with no new play costs
# nothing. Measured Sep 13 2026: the full check was ~450 ms per game on a 4 ms pipeline, and the live poller
# fetches every open game one after another every 10 s.
_ALIGN_CACHE = {}
_ALIGN_CACHE_MAX = 2000


def _aligned(mine_rows, their_plays):
    sig = repr(([(r.get("play_text"), r.get("clock"), r.get("down"), r.get("distance"),
                  r.get("home_score"), r.get("away_score")) for r in mine_rows],
                [(t.get("text"), t.get("clock"), t.get("drive_text"), t.get("home_score"), t.get("away_score"))
                 for t in their_plays]))
    key = hashlib.sha1(sig.encode("utf-8")).hexdigest()
    hit = _ALIGN_CACHE.get(key)
    if hit is None:
        hit = M._align_quarter(mine_rows, their_plays)
        if len(_ALIGN_CACHE) >= _ALIGN_CACHE_MAX:
            _ALIGN_CACHE.clear()
        _ALIGN_CACHE[key] = hit
    return hit


def without_added_rows(payload):
    """The play payload minus rows the NCAA check ADDED, for clients that take new plays by count (see main.py
    /game/{id}/plays). Returns a NEW dict - the cached payload is shared by every client and must not change."""
    entries = (payload or {}).get("entries") or []
    if not any(e.get("ncaa_status") == "added" for e in entries):
        return payload
    out = dict(payload)
    out["entries"] = [e for e in entries if e.get("ncaa_status") != "added"]
    return out


def verify_entries(entries, pbp, home_name, away_name, clock_src=None):
    """Check ESPN's entries against NCAA's play-by-play IN PLACE. Returns a summary dict. Never raises on odd data -
    the caller also wraps it, because a second source must never break play delivery."""
    clock_src = clock_src or {}
    plays = [p for p in (pbp.get("plays") or []) if p.get("text")]
    summary = {"available": bool(plays), "verified": 0, "fixed": 0, "added": 0, "unverified": 0,
               "skipped_scoring_adds": 0, "skipped_duplicate_adds": 0, "rejected_pairs": 0, "review_count": 0, "examples": []}
    if not plays or not entries:
        return summary
    teams = pbp.get("teams") or {}
    team_name = {str((teams.get("home") or {}).get("team_id") or ""): home_name,
                 str((teams.get("away") or {}).get("team_id") or ""): away_name}

    for e in entries:
        e["ncaa_status"] = ""
        e["ncaa_changes"] = []

    quarters = sorted({M._quarter(e.get("quarter")) for e in entries} - {0})
    last_q = max(quarters) if quarters else 0
    pairs, to_add = {}, []
    for q in quarters:
        mine = [(i, e) for i, e in enumerate(entries)
                if M._quarter(e.get("quarter")) == q and not M.is_admin_line(e.get("play_text", ""))]
        theirs = [t for t in plays if M._quarter(t.get("quarter")) == q and not M.is_admin_line(t.get("text", ""))]
        events, last = [], None
        for a, b in _aligned([e for _i, e in mine], theirs):
            if a is not None:
                i = mine[a][0]
                last = i
                if b is not None and _good_pair(entries[i], theirs[b], clock_src.get(str(entries[i].get("espn_play_id")))):
                    pairs[i] = theirs[b]
                    events.append(("pair", i))
                else:
                    if b is not None:
                        summary["rejected_pairs"] += 1
                    events.append(("espn", i))
            else:
                events.append(("ncaa", (last, theirs[b])))
        # A play NCAA has and ESPN does not is only added once ESPN has moved past it (3 lined-up plays after it,
        # or a finished quarter) - otherwise it may simply not have reached ESPN yet.
        for k, ev in enumerate(events):
            if ev[0] == "ncaa" and (q < last_q or sum(1 for x in events[k + 1:] if x[0] == "pair") >= 3):
                to_add.append((q, ev[1][0], ev[1][1]))

    # Which spot prefix is whose own side, learned from lined-up plays that already agree on the yard.
    side_votes = Counter()
    for i, t in pairs.items():
        _d, _dist, sd, yd = situation(t.get("drive_text"))
        fp = _int(entries[i].get("field_position"))
        if sd and yd and yd != 50 and fp and abs(fp) == yd:
            side_votes[(sd, entries[i].get("possession"), "own" if fp < 0 else "opp")] += 1

    def ncaa_spot(entry, t):
        _d, _dist, sd, yd = situation(t.get("drive_text"))
        if yd is None:
            return None
        if yd == 50:
            return -50
        fp = _int(entry.get("field_position")) or 0
        if not sd:                                    # no side given: the yard, on ESPN's side
            return yd if fp > 0 else -yd
        own = side_votes[(sd, entry.get("possession"), "own")]
        opp = side_votes[(sd, entry.get("possession"), "opp")]
        if own == opp:
            return None                               # side unknown - leave ESPN's spot alone
        return -yd if own > opp else yd

    def change(i, field, new, why):
        e = entries[i]
        e["ncaa_changes"].append({"field": field, "old": e.get(field), "new": new, "why": why})
        if len(summary["examples"]) < 8:
            summary["examples"].append("Q%s %s %s: %s -> %s (%s)" % (e.get("quarter"), e.get("clock"), field,
                                                                   e.get(field), new, why))
        e[field] = new

    for i, e in enumerate(entries):
        text = str(e.get("play_text") or "")
        if M.is_admin_line(text):
            continue
        t = pairs.get(i)
        if t is None:
            # NCAA writes "<Team> drive start at 14:57" in place of some first snaps and kickoffs.
            mine = M.clock_secs(e.get("clock"))
            q = M._quarter(e.get("quarter"))
            starts = [M.clock_secs(m.group(1)) for p in plays if M._quarter(p.get("quarter")) == q
                      for m in [_DRIVE_START.search(str(p.get("text") or ""))] if m]
            if mine is not None and any(s is not None and abs(s - mine) <= 3 for s in starts):
                e["ncaa_status"] = "verified"
                summary["verified"] += 1
            else:
                e["ncaa_status"] = "unverified"
                summary["unverified"] += 1
            continue
        down = str(e.get("down") or "").strip().upper()
        if down.isdigit() and not _TIMEOUT.match(text):
            nd, ndist, _sd, _yd = situation(t.get("drive_text"))
            if nd and nd != down:
                change(i, "down", nd, "NCAA")
            if ndist is not None and ndist != _int(e.get("distance")):
                change(i, "distance", ndist, "NCAA")
            nfp = ncaa_spot(e, t)
            if nfp is not None and nfp != _int(e.get("field_position")):
                change(i, "field_position", nfp, "NCAA")
        # Clock order step 3: NCAA's clock where ours is only a best guess - when it fits between its neighbours.
        if clock_src.get(str(e.get("espn_play_id"))) == "guess" and down not in ("EP", "2PT"):
            ns = _ncaa_secs(t)
            if ns is not None:
                q = e.get("quarter")
                before = [M.clock_secs(x.get("clock")) for x in entries[:i] if x.get("quarter") == q]
                after = [M.clock_secs(x.get("clock")) for x in entries[i + 1:] if x.get("quarter") == q]
                hi = before[-1] if before and before[-1] is not None else 15 * 60
                lo = after[0] if after and after[0] is not None else 0
                if lo <= ns <= hi and ns != M.clock_secs(e.get("clock")):
                    change(i, "clock", "%d:%02d" % divmod(ns, 60), "NCAA clock")
        e["ncaa_status"] = "fixed" if e["ncaa_changes"] else "verified"
        summary["fixed" if e["ncaa_changes"] else "verified"] += 1

    # Add NCAA's extra plays, last first so earlier positions stay valid.
    inserts = []
    for q, after, t in to_add:
        text = str(t.get("text") or "")
        if _SCORING.search(text):
            summary["skipped_scoring_adds"] += 1      # a score changes every later board - left for review
            continue
        if _already_have(entries, q, t):
            summary["skipped_duplicate_adds"] += 1    # NCAA listed a play ESPN already has twice
            continue
        if after is None:
            pos = next((k for k, e in enumerate(entries) if M._quarter(e.get("quarter")) == q), len(entries))
        else:
            pos = after + 1
        inserts.append((pos, q, t))
    for pos, q, t in sorted(inserts, key=lambda x: x[0], reverse=True):
        text = _espn_names(t.get("text"))
        prev = next((entries[k] for k in range(pos - 1, -1, -1) if M._quarter(entries[k].get("quarter")) == q), None)
        nxt = entries[pos] if pos < len(entries) else prev
        nd, ndist, _sd, yd = situation(t.get("drive_text"))
        poss = team_name.get(str(t.get("team_id") or "")) or (prev or {}).get("possession", "")
        g = _GAIN.search(text)
        gain = (int(g.group(1)) * (1 if g.group(2).lower() == "gain" else -1)) if g else 0
        if ndist is None:
            if nd == "1":
                ndist = 10
            elif prev and prev.get("possession") == poss and _int(prev.get("distance")) is not None:
                ndist = max(1, _int(prev.get("distance")) - (_int(prev.get("gain")) or 0))
            else:
                ndist = 10
        spot = ncaa_spot({"possession": poss, "field_position": (prev or {}).get("field_position")}, t)
        secs = _ncaa_secs(t)
        entry = {
            "home_score": (nxt or {}).get("home_score", 0),
            "away_score": (nxt or {}).get("away_score", 0),
            "clock": ("%d:%02d" % divmod(secs, 60)) if secs is not None else (prev or {}).get("clock", "0:00"),
            "quarter": (prev or nxt or {}).get("quarter", q),
            "down": nd or "1",
            "distance": ndist,
            "gain": gain,
            "field_position": spot if spot is not None else (prev or {}).get("field_position", 0),
            "possession": poss,
            "run_clock": "No",
            "home_time_out": "No",
            "away_time_out": "No",
            "play_text": text,
            "wallclock": "",
            "espn_play_id": "",
            "espn_seq": None,
            "_forced_key": _added_key(q, text),
            "ncaa_status": "added",
            "ncaa_changes": [{"field": "row", "old": "", "new": "added", "why": "NCAA has this play, ESPN does not"}],
        }
        entries.insert(pos, entry)
        summary["added"] += 1
        if len(summary["examples"]) < 8:
            summary["examples"].append("Q%s %s added from NCAA: %s" % (entry["quarter"], entry["clock"], text[:60]))

    summary["review_count"] = summary["fixed"] + summary["added"]
    return summary
