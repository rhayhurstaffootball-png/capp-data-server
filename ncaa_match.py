"""Match CAPP's ESPN-sourced rows against NCAA.com's play-by-play.

Two jobs, both of which produce a PROPOSAL for a human to accept - nothing here
edits a game:

    match_timeouts()  which stoppages NCAA says were charged to a team
    propose_clocks()  what the clock really was on a run of stuck plays

⚠ WHY THIS IS NOT A POSITIONAL WALK. NCAA's list is longer than ours: it carries
drive-start markers, "UMD ball on UMD35." and the coin toss. 219 NCAA rows
against our 182 on Hampton @ Maryland. Walking the two lists together drifts, and
that exact mistake once paired our Q1 4:36 against their 2:58. Everything here
matches on CONTENT, inside one quarter, and is bounded by the good clocks either
side of the run.

⚠ WHY THE BOUNDS ARE LOAD-BEARING. Without them the matcher picked a play from
the wrong end of the same quarter that happened to share a down and a player, and
proposed turning a Q1 4:36 into 13:49. A candidate must sit between the last good
clock before the run and the first good clock after it, and a run may never climb
back up.

NAME FORMATS DIFFER. ESPN writes "#20 A.Pryear rush left", NCAA writes
"Pryear,Amare rush left". The surname is the part both agree on, so that is what
is compared - never the initial, never the number.
"""

import re
import unicodedata

# ── small shared helpers ─────────────────────────────────────────────────────

def norm_clock(c) -> str:
    """'09:44' -> '9:44'. NCAA pads to MM:SS, CAPP stores M:SS."""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(c or ""))
    return f"{int(m.group(1))}:{m.group(2)}" if m else str(c or "").strip()


def clock_secs(c):
    """Clock as seconds remaining, or None if it is not a clock."""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(c or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def text_clock(play_text):
    """The clock written INSIDE the play text, e.g. 'Timeout MD, clock 04:35.'

    Worth having: on one measured row ESPN's clock FIELD said 4:09 while its own
    text said 04:35, and NCAA agreed with the text. When the two disagree the
    text is the better key to match on.
    """
    t = str(play_text or "")
    m = re.search(r"clock\s+(\d{1,2}:\d{2})", t, re.I)
    if m:
        return norm_clock(m.group(1))
    # Both houses also prefix the line with the clock in brackets - "(01:03) No
    # Huddle ...". That is the field that separates two near-identical carries by
    # the same runner, which is exactly where the alignment used to slip.
    m = re.match(r"\s*\((\d{1,2}:\d{2})\)", t)
    return norm_clock(m.group(1)) if m else ""


def _fold(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def _quarter(v):
    try:
        return int(str(v).strip() or 0)
    except (TypeError, ValueError):
        return 0


# ── timeouts ─────────────────────────────────────────────────────────────────

def match_timeouts(rows, ncaa_timeouts, tolerance=90):
    """Pair our timeout rows with NCAA's, best first, each NCAA row used once.

    `rows`: dicts with quarter, clock, play_text (index/anything else is carried
    through untouched). `ncaa_timeouts`: the server's /ncaa/timeouts list.

    Returns a list, one entry per input row, in input order:
        {"row": <the row>, "ncaa": <matched dict or None>, "how": str}

    `how` is "clock", "text-clock", "near ±Ns" or "no match" - shown to the coach
    so a weak pairing is visible rather than implied.
    """
    used = set()
    pairs = []

    def candidates(row):
        q = _quarter(row.get("quarter"))
        return [(i, t) for i, t in enumerate(ncaa_timeouts)
                if i not in used and _quarter(t.get("quarter")) == q]

    # Exact clock matches first, across ALL rows, before any fuzzy pairing gets
    # to consume an NCAA row a later row would have matched exactly.
    result = [None] * len(rows)
    for pass_no, key in ((1, "clock"), (2, "text-clock")):
        for ri, row in enumerate(rows):
            if result[ri] is not None:
                continue
            want = norm_clock(row.get("clock")) if key == "clock" else text_clock(row.get("play_text"))
            if not want:
                continue
            for i, t in candidates(row):
                if norm_clock(t.get("clock")) == want:
                    used.add(i)
                    result[ri] = {"row": row, "ncaa": t, "how": key}
                    break

    for ri, row in enumerate(rows):
        if result[ri] is not None:
            continue
        mine = clock_secs(row.get("clock"))
        best, best_gap, best_i = None, None, None
        if mine is not None:
            for i, t in candidates(row):
                theirs = clock_secs(t.get("clock"))
                if theirs is None:
                    continue
                gap = abs(theirs - mine)
                if gap <= tolerance and (best_gap is None or gap < best_gap):
                    best, best_gap, best_i = t, gap, i
        if best is not None:
            used.add(best_i)
            result[ri] = {"row": row, "ncaa": best, "how": f"near ±{best_gap}s"}
        else:
            result[ri] = {"row": row, "ncaa": None, "how": "no match"}

    pairs.extend(result)
    return pairs


def charged_side(ncaa_entry):
    """'home' | 'away' | None (officials) | 'unknown'. None means: still show the
    red timeout board, just do not burn one of that team's three."""
    if not ncaa_entry:
        return "unknown"
    ch = ncaa_entry.get("charged")
    return ch if ch in ("home", "away", "unknown") else None


# ── clock repair ─────────────────────────────────────────────────────────────

# ⚠ ORDER MATTERS - the first hit wins, so the specific phrases come before
# the generic ones ("kick attempt" before "kickoff", "field goal" before "kick").
_ACTIONS = ("incomplete", "sacked", "sack", "intercepted", "fumble", "penalty",
            "timeout", "kick attempt", "extra point", "pat", "field goal",
            "kickoff", "kicks", "punt", "kneel", "complete", "rush", "run")

# The two houses word the same play very differently. Measured against real
# games: every false "ESPN is missing a play" on two full games was a PAT -
# NCAA writes "Spetic,Gianni kick attempt good (H: ...)" where we write
# "Extra Point Good", and the two share no scoreable words at all.
_ACTION_SYNONYMS = {
    "kicks": "kickoff",
    "sack": "sacked",
    "run": "rush",
    "kick attempt": "extra point",
    "pat": "extra point",
}


def _surnames(text):
    """Surnames from either house's format.

    ESPN: '#3 T.Taylor pass complete'      -> taylor
    NCAA: 'Taylor,Trey pass complete'      -> taylor
    """
    t = str(text or "")
    out = set()
    for m in re.finditer(r"\b[A-Z]\.([A-Z][A-Za-z\-']+)", t):
        out.add(m.group(1).lower())
    for m in re.finditer(r"\b([A-Z][A-Za-z\-']{2,}),\s*[A-Z]", t):
        out.add(m.group(1).lower())
    return out


def _action(text):
    t = _fold(text)
    for w in _ACTIONS:
        if w in t:
            return _ACTION_SYNONYMS.get(w, w)
    return ""


_STOP = {"the", "for", "to", "at", "of", "a", "and", "on", "no", "yards", "yard"}


def _tokens(text):
    return {w for w in re.findall(r"[a-z]{3,}", _fold(text)) if w not in _STOP}


def _drive_meta(drive_text):
    """NCAA's '3 and 9 at 14' -> (down, distance, yardline) as strings.

    ⚠ Sometimes incomplete or malformed, which is one reason a human confirms
    every correction - a missing side of the field makes '14' ambiguous.
    """
    m = re.match(r"\s*(\d)\s+and\s+(\d+)\s+at\s+(\S+)", str(drive_text or ""), re.I)
    return (m.group(1), m.group(2), m.group(3)) if m else ("", "", "")


def score_pair(row, ncaa_play):
    """How much do these two describe the same play? 0-12, higher is better."""
    s = 0
    mine_txt, theirs_txt = row.get("play_text", ""), ncaa_play.get("text", "")

    mine_names, their_names = _surnames(mine_txt), _surnames(theirs_txt)
    if mine_names & their_names:
        s += 3

    a1, a2 = _action(mine_txt), _action(theirs_txt)
    if a1 and a1 == a2:
        s += 2
        # Some plays are named, not described: we write "Extra Point Good" where
        # they write "Gerlach,Daniel kick attempt good (H: ..., LS: ...)". There
        # is no shared name and barely a shared word, so without this the two
        # never pair and every PAT reads as a missing play. The play type plus
        # the position in the quarter is identification enough for these.
        if a1 in ("extra point", "kickoff", "punt", "field goal"):
            s += 3

    t1, t2 = _tokens(mine_txt), _tokens(theirs_txt)
    if t1 and t2:
        overlap = len(t1 & t2) / max(1, min(len(t1), len(t2)))
        s += 3 if overlap >= 0.6 else (2 if overlap >= 0.4 else (1 if overlap >= 0.25 else 0))

    d, dist, _yard = _drive_meta(ncaa_play.get("drive_text"))
    if d and str(row.get("down", "")).strip() == d:
        s += 2
    if dist and str(row.get("distance", "")).strip() == dist:
        s += 1

    # THE CLOCK IS THE TIE-BREAKER. Two carries by the same runner for similar
    # yardage score identically on words alone, and the aligner then pairs the
    # wrong one and reports a play we plainly have as "missing". Use the row's
    # clock, or the one written into the text, whichever exists.
    c1 = clock_secs(row.get("clock")) or clock_secs(text_clock(mine_txt))
    c2 = clock_secs(ncaa_play.get("clock")) or clock_secs(text_clock(theirs_txt))
    if c1 is not None and c2 is not None:
        gap = abs(c1 - c2)
        s += 3 if gap <= 15 else (1 if gap <= 45 else (-3 if gap > 120 else 0))

    # Scores are a strong constraint: they only change on a scoring play, so a
    # mismatch means we are looking at a different part of the game.
    try:
        if (str(row.get("home_score", "")).strip() == str(ncaa_play.get("home_score", "")).strip()
                and str(row.get("away_score", "")).strip() == str(ncaa_play.get("away_score", "")).strip()):
            s += 1
    except Exception:
        pass
    return s


def find_stuck_runs(rows, min_len=3):
    """Index ranges where the clock does not move for `min_len`+ plays in one
    quarter. This is what a stalled stat crew looks like in the data."""
    runs, start = [], 0
    for i in range(1, len(rows) + 1):
        same = (i < len(rows)
                and norm_clock(rows[i].get("clock")) == norm_clock(rows[start].get("clock"))
                and _quarter(rows[i].get("quarter")) == _quarter(rows[start].get("quarter")))
        if not same:
            if i - start >= min_len:
                runs.append((start, i - 1))
            start = i
    return runs


def propose_clocks(rows, ncaa_plays, selection, min_score=6):
    """Propose a clock for each SELECTED row, bounded by its healthy neighbours.

    ⚠ HOW MANY NCAA ROWS CARRY A CLOCK VARIES BY STAT CREW. Across eight games
    from Sep 5 2026 it ran from 34% (Ohio St.) and 37% (Maryland) to 94% (Air
    Force) and 100% (Penn St.). On a well-clocked game nearly every stuck row can
    be repaired from source; on a sparse one the clock survives only on drive
    starts, kicks, punts, scores and timeouts, and the scrimmage plays inside a
    stuck run carry none. So NCAA cannot be assumed to hold the right time for
    every stuck row, and any code that assumes it is inventing data.

    What it CAN do, and what this returns, is two clearly separated things:

        source "ncaa"       this row matched an NCAA row that carries a real
                            clock. This is source data.
        source "estimated"  no NCAA clock for this row, so the time is
                            interpolated between the NCAA anchors either side.
                            On the one run with independently verified times it
                            landed within about 5-15 seconds. It is an ESTIMATE
                            and the UI must not present it as anything else.

    Rows that can be neither are returned with new="" and left alone.
    """
    proposals = []
    sel = sorted(set(int(i) for i in selection))
    if not sel:
        return proposals

    anchors = [p for p in ncaa_plays if clock_secs(p.get("clock")) is not None]
    used = set()

    for group in _contiguous(sel):
        q = _quarter(rows[group[0]].get("quarter"))
        ceiling = 15 * 60
        for k in range(group[0] - 1, -1, -1):
            if _quarter(rows[k].get("quarter")) != q:
                break
            v = clock_secs(rows[k].get("clock"))
            if v is not None and k not in sel:
                ceiling = v
                break
        floor = 0
        for k in range(group[-1] + 1, len(rows)):
            if _quarter(rows[k].get("quarter")) != q:
                break
            v = clock_secs(rows[k].get("clock"))
            if v is not None and k not in sel:
                floor = v
                break

        # ---- pass 1: real NCAA clocks, content-matched, monotonic ------------
        found = {}
        cap = ceiling
        for idx in group:
            row = rows[idx]
            best, best_s, best_j = None, 0, None
            for j, np_ in enumerate(anchors):
                if j in used or _quarter(np_.get("quarter")) != q:
                    continue
                cs = clock_secs(np_.get("clock"))
                if cs > cap or cs < floor:
                    continue
                sc = score_pair(row, np_)
                if sc > best_s:
                    best, best_s, best_j = np_, sc, j
            if best is not None and best_s >= min_score:
                used.add(best_j)
                found[idx] = {"secs": clock_secs(best.get("clock")),
                              "score": best_s, "text": best.get("text", "")}
                cap = found[idx]["secs"]        # a run may never climb back up

        # ---- pass 2: interpolate the gaps between anchors --------------------
        known = [(-1, ceiling)] + [(i, found[i]["secs"]) for i in sorted(found)] +                 [(len(rows), floor)]
        est = {}
        for (i0, t0), (i1, t1) in zip(known, known[1:]):
            gap = [i for i in group if i0 < i < i1]
            if not gap or t0 is None or t1 is None or t1 > t0:
                continue
            step = (t0 - t1) / (len(gap) + 1)
            for n, idx in enumerate(gap, start=1):
                est[idx] = int(round(t0 - step * n))

        for idx in group:
            row = rows[idx]
            old = norm_clock(row.get("clock"))
            if idx in found:
                new_c = "%d:%02d" % divmod(found[idx]["secs"], 60)
                proposals.append({
                    "index": idx, "quarter": q, "old": old, "new": new_c,
                    "source": "ncaa", "score": found[idx]["score"],
                    "ncaa_text": found[idx]["text"],
                    "note": "already correct" if new_c == old else "",
                })
            elif idx in est:
                new_c = "%d:%02d" % divmod(max(0, est[idx]), 60)
                proposals.append({
                    "index": idx, "quarter": q, "old": old, "new": new_c,
                    "source": "estimated", "score": 0, "ncaa_text": "",
                    "note": "estimated between NCAA times"
                            if new_c != old else "already correct",
                })
            else:
                proposals.append({
                    "index": idx, "quarter": q, "old": old, "new": "",
                    "source": "", "score": 0, "ncaa_text": "",
                    "note": "no NCAA time nearby - left alone",
                })
    return proposals


def _contiguous(sorted_indices):
    """[3,4,5,9,10] -> [[3,4,5],[9,10]]"""
    groups, run = [], [sorted_indices[0]]
    for i in sorted_indices[1:]:
        if i == run[-1] + 1:
            run.append(i)
        else:
            groups.append(run); run = [i]
    groups.append(run)
    return groups


# ── missing / duplicate play detection ───────────────────────────────────────
# ⚠ GREEDY BEST-MATCH IS NOT ENOUGH HERE, and the failure is not subtle. A game
# has a dozen near-identical lines ("Spetic,Gianni kickoff 65 yards to the
# TXST00, Touchback."). Scored independently they all look equally good, so the
# first of our rows claims the wrong partner and the leftovers orphan each
# other. Measured on two full games that are NOT missing any plays, greedy
# reported 8-14 phantom "missing" rows - all kickoffs, PATs and touchdowns we
# demonstrably HAVE (kickoffs 12 v 12, touchbacks 8 v 8).
#
# Order fixes it. Both houses list a game in the same order, so a global
# alignment inside each quarter (Needleman-Wunsch, similarity = score_pair) can
# only pair the 3rd kickoff with the 3rd kickoff. Cost is trivial at ~50 rows a
# quarter.

_GAP = -2          # cost of leaving a row unpaired
_MIN_SIM = 4       # below this, two rows are not the same play at any price


def _align_quarter(ours, theirs):
    """Needleman-Wunsch. Returns [(i or None, j or None)] in play order."""
    n, m = len(ours), len(theirs)
    F = [[0] * (m + 1) for _ in range(n + 1)]
    # Each pair is scored ONCE and reused by the walk back. score_pair is the whole cost of this function, and
    # scoring every pair twice doubled it - the server now runs this on every live update (Sep 13 2026).
    S = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        F[i][0] = F[i - 1][0] + _GAP
    for j in range(1, m + 1):
        F[0][j] = F[0][j - 1] + _GAP
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            sim = S[i][j] = score_pair(ours[i - 1], theirs[j - 1])
            diag = F[i - 1][j - 1] + (sim if sim >= _MIN_SIM else 2 * _GAP)
            F[i][j] = max(diag, F[i - 1][j] + _GAP, F[i][j - 1] + _GAP)

    out, i, j = [], n, m
    while i > 0 or j > 0:
        if i > 0 and j > 0:
            sim = S[i][j]
            diag = F[i - 1][j - 1] + (sim if sim >= _MIN_SIM else 2 * _GAP)
            if F[i][j] == diag:
                out.append((i - 1, j - 1)); i -= 1; j -= 1; continue
        if i > 0 and F[i][j] == F[i - 1][j] + _GAP:
            out.append((i - 1, None)); i -= 1; continue
        out.append((None, j - 1)); j -= 1
    out.reverse()
    return out


# Lines the backup source carries that are not plays. Deliberately narrow: a row
# that does NOT match here counts as a play, so a gap in this list costs a false
# alarm, never a silently ignored missing play.
_ADMIN_LINE = re.compile(
    r"^\s*("
    r".*\bdrive start\b"
    r"|.*\bball on\b"
    r"|.*\bwins toss\b|.*\bdefer(s|red)?\b|.*\bwill receive\b"
    r"|start of \w+ (quarter|period)\b|end of \w+ (quarter|period)\b"
    r"|.*\bend of (game|half|regulation)\b|.*\bstart of (game|half|overtime)\b"
    r"|timeout\b|officials timeout\b"
    r"|.*\binjured on the play\b"
    # NCAA writes a disqualification as its own line ("Air Force C.Paterson has been disqualified") after the
    # penalty play. Not a play: the live NCAA check added two of them to Air Force as boards (Sep 13 2026).
    # ⚠ ONLY a line that is nothing but "<team> <player> has been disqualified". NCAA also appends the same words
    # to the end of the real penalty PLAY ("(13:05) Shotgun ... NO PLAY. Air Force C.Paterson has been
    # disqualified"), and that play must stay a play - so no digits or brackets are allowed before the words.
    r"|[A-Za-z .'\-]+\bhas been disqualified\b\.?\s*$"
    r"|score gap at period boundary"
    r")", re.I)


def is_admin_line(text):
    """True for a row that is bookkeeping rather than a play."""
    return bool(_ADMIN_LINE.match(str(text or "")))


def compare_plays(rows, ncaa_plays, skip=None):
    """Line up our game against the backup source and report the differences.

    `skip(text) -> bool` drops rows that are not plays on either side (drive
    starts, "UMD ball on UMD35.", the coin toss, timeouts).

    Returns {"missing": [...], "extra": [...], "matched": n} where `missing` is
    a backup play with no counterpart of ours - each carrying `after_index`, the
    row of OURS it should follow, so the caller can offer to insert it in place.
    """
    skip = skip or is_admin_line
    mine = [(i, r) for i, r in enumerate(rows) if not skip(r.get("play_text", ""))]
    theirs = [t for t in ncaa_plays if not skip(t.get("text", ""))]

    quarters = sorted({_quarter(r.get("quarter")) for _i, r in mine} |
                      {_quarter(t.get("quarter")) for t in theirs})
    missing, extra, matched = [], [], 0
    for q in quarters:
        qm = [(i, r) for i, r in mine if _quarter(r.get("quarter")) == q]
        qt = [t for t in theirs if _quarter(t.get("quarter")) == q]
        last_ours = None
        for a, b in _align_quarter([r for _i, r in qm], qt):
            if a is not None and b is not None:
                matched += 1
                last_ours = qm[a][0]
            elif a is not None:
                extra.append({"index": qm[a][0], "quarter": q,
                              "text": qm[a][1].get("play_text", ""),
                              "clock": norm_clock(qm[a][1].get("clock"))})
                last_ours = qm[a][0]
            else:
                t = qt[b]
                missing.append({"after_index": last_ours, "quarter": q,
                                "clock": norm_clock(t.get("clock")),
                                "text": t.get("text", ""),
                                "drive_text": t.get("drive_text", ""),
                                "home_score": t.get("home_score"),
                                "away_score": t.get("away_score")})
    return {"missing": missing, "extra": extra, "matched": matched}


# ── assumptions, for when the source has no answer ───────────────────────────
# Roger, Sep 6 2026: "Like after a Kick there is usually an officials timeout and
# if the clock is exactly 2:00 then thats an officials time out."
#
# MEASURED, not assumed. Ground truth = the backup source's own labels on 201
# timeouts across the ~23 games in 26 whose crew recorded them:
#
#     clock is exactly 2:00                       27 fired, 26 right   96%
#     after a score or kickoff                    30 fired, 26 right   87%
#       ...same, excluding OT and the last 2:00 of Q4
#                                                 29 fired, 28 right   97%
#     both together                               56 fired, 54 right   96%
#     clock is exactly 15:00                       2 fired,  0 right    0%  <- REJECTED
#
# THE LATE-GAME EXCLUSION IS THE WHOLE TRICK. Every failure of the raw
# after-a-score rule was a trailing team calling timeout right after a score in
# the last two minutes or in overtime - which is football, not noise. Excluding
# that window took it from 87% to 97%.
#
# 15:00 was rejected outright: both times it fired it was a real team timeout.
#
# \u26a0 These only ever set a DEFAULT, and only where the source has nothing to say.
# A wrong "officials" hands a team back a timeout it really spent, so the coach
# still sees every row.

_SCORING_TEXT = re.compile(r"touchdown|field goal.*good|kick attempt good|extra point", re.I)
_KICKOFF_TEXT = re.compile(r"\bkickoff\b|\bkicks off\b|\bkicks\b", re.I)
_NOT_A_PLAY = re.compile(r"drive start|ball on|wins toss|start of \w+ (quarter|period)"
                         r"|end of \w+ (quarter|period)|end of game"
                         r"|score gap at period boundary", re.I)


def _previous_play(rows, index):
    """Nearest earlier row that is a real play - not a timeout, not bookkeeping."""
    for j in range(index - 1, -1, -1):
        t = str(rows[j].get("play_text", ""))
        if _NOT_A_PLAY.search(t) or t.strip().lower().startswith(("timeout", "officials timeout")):
            continue
        return rows[j]
    return None


def guess_official(rows, index):
    """(True, reason) if this timeout row can be assumed to be officials/TV.

    Returns (False, "") when no rule applies - which is most of them. Silence is
    the right answer far more often than a guess.
    """
    row = rows[index]
    q = _quarter(row.get("quarter"))
    cs = clock_secs(row.get("clock"))

    if norm_clock(row.get("clock")) == "2:00":
        return True, "two-minute"

    # A trailing team stops the clock right after a score late on, so the
    # after-a-score rule must not apply in the last two minutes or in overtime.
    late = q >= 5 or (q == 4 and cs is not None and cs <= 120)
    if not late:
        prev = _previous_play(rows, index)
        ptxt = str(prev.get("play_text", "")) if prev else ""
        if _SCORING_TEXT.search(ptxt):
            return True, "after a score"
        if _KICKOFF_TEXT.search(ptxt):
            return True, "after a kick"
    return False, ""
