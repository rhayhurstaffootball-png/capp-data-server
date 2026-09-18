"""CBS check - ESPN is the play data, CBS is the comparison source (Roger, Sep 14 2026).

Roger, Sep 14 2026: "ESPN = Primary CBS = Primary Backup for Comparison NCAA = Secondary Backup for ESPN Bad Plays and
CBS not Posting Play By Play." His rules for the check:
  clocks      "ESPN Typed Data > ESPN Auto Data > CBS Data > NCAA Data" - "ESPN First ... we dont Automaticall just go to
              CBS... CBS is a backup and to be used mostly for Backup, Replacement and Repair". A play keeps ESPN's clock
              (the "(MM:SS)" typed in the text, then ESPN's own clock) unless it is messed up: "If there is a stuck
              Clock, no Clock, A Clock that is Crazy off
              then We Check CBS" / "If we get a stuck Clock error we check CBS to See if the clock is stuck for real".
              Checked: a clock the server had to GUESS (ESPN's was missing / stuck / did not fit) and every play of a
              stuck run (the "Clock stuck" QC error, typed snaps included). CBS's clock, else NCAA's (when NCAA's
              play-by-play is given) - each only when it fits between the plays around it. "Crazy off" for a typed snap:
              threshold asked, not built.
  spots       "SPOTS ESPN WINS" / down and distance "ESPN WINS" - except when CBS AND NCAA agree ESPN is off "in a
              noticable fashio[n]": "A Down is always a difference.. it should follow the flow"; distance / spot "I would
              say 2... I have seen it be off by 1 but 2 yards is a long way". So a different down (unless the plays
              around it follow ESPN's down - then nothing on that row changes) or 2+ yards takes CBS + NCAA's value.
              CBS alone never changes a play ESPN has.
  adds        "CBS can add a full Play to the game, as long as its not a duplicate ... CBS WINS" - a play ESPN does not
              have is added with CBS's down, distance, spot and clock.
  same play   "if the clock is a few seconds off and the Down and Distance and Field Position, and the play description
              are the same.. then I would expect that to be the same play". A distance 1 yard off, or the spot on the
              other side of the 50: "the same play and there was a typo". The clock is never part of the test.
A game CBS has no play-by-play for never reaches this module - espn_fetcher runs the NCAA check instead.

Everything a client reads is written in the SAME fields the NCAA check writes (ncaa_status verified / fixed /
unverified / added / superseded, ncaa_changes, "gap:" keys through _forced_key), so SBENTRY and the installed app need
no change: installed clients never get added rows (ncaa_check.without_added_rows), SBENTRY takes them by key.

MEASURED Sep 14 2026 before building (_dev_tools/game_replay_test/measure_cbs.py, 79 Sep 12 games):
  * CBS has no play-by-play for 10 of 79.   * 11,245 ESPN plays lined up with CBS's, 114 rejected pairs.
  * CBS's clock equals ESPN's typed snap on 3,197 of 9,741 lined-up plays; 5,141 are 1-5 s off.
  * Of 85 plays CBS has and ESPN seemed not to, 54 were ESPN plays by the same-play test (51 with clocks more than a
    minute apart - ESPN's clock was 0:00, stuck or typed minutes off), 13 more by the typo rule, 18 real candidates
    (incl. Nebraska Q1 5:34 Ridley sack, missing from ESPN in Roger's answer key).
"""
import copy
import hashlib
import json
import re
from collections import Counter

import ncaa_check
import ncaa_match as M

PLAY_KINDS = ("play", "kickoff", "extra_point", "two_point")
MIN_PAIR_SCORE = 6            # backup_resolve's value, graded on Bleacher Report pastes (CBS words plays the same way)
DISTANCE_TYPO = 1             # Roger: "Wouldnt surprise me if the play is 1 yard off"
STUCK_RUN = 3                 # the "Clock stuck" QC error - espn_fetcher._QC_STUCK_THRESH. Was 4 until Sep 18 2026:
                              # Syracuse @ Pitt Q4 had three snaps on 3:09 the check never looked at. MEASURED at 3 on
                              # the Sep 12 corpus + Pitt: 47 runs (17 at 4), 20 confirmed stuck for real and left
                              # alone, CBS clock fixes 506 -> 511, backwards clocks 16 -> 16, games worse 0.
SAME_CLOCK_SECONDS = 5        # CBS within this of a stuck clock on every play = the clock was stuck for real
BACKWARDS_MARGIN = 5          # an ESPN clock this far above the row before it runs backwards (every_game_check.BACK_MARGIN)
SNAP_YARDS = 1                # same snap: yard line within this (a typo), see same_snap()
# Roger, Sep 14 2026: CBS + NCAA beat ESPN "If the ESPN versions are off in a noticable fashio[n]" / "A Down is always a
# difference" / distance and spot: "I would say 2... because if its off I have seen it be off by 1 but 2 yards is a long
# way". So 2+ yards off = fixed when CBS and NCAA agree; 1 yard stays ESPN's.
NOTICEABLE_YARDS = 2
# A stretch of typed ESPN clocks all off CBS's by this much in the same direction takes CBS's clocks (see the
# "off as a run" step in verify_entries). Module-level so a measurement can switch the rule off (OFF_RUN_MIN = 999).
OFF_RUN_SECONDS = 30
OFF_RUN_MIN = 3
_NO_CLOCK_DOWNS = ("EP", "2PT")
# A penalty-only CBS line ESPN does not have (a dead-ball foul, no snap) is NOT added until Roger rules: an extra board with
# no film clip moved every later clip on SMU Sep 12 (the 9:57 personal foul), and whether dead-ball fouls get a board is
# his per-school film question (MEMORY: offsetting penalties). Asked Sep 14 2026.
ADD_PENALTY_ONLY = False

_SNAP = re.compile(r"^\s*\((\d{1,2}):(\d{2})\)")
_PEN_ONLY = re.compile(r"^\s*(\(\d{1,2}:\d{2}\)\s*)?penalty\b", re.I)
_TIMEOUT = re.compile(r"^\s*(\(\d{1,2}:\d{2}\)\s*)?(officials\s+)?time\s*out", re.I)
_SCORING = re.compile(r"touchdown|field goal (attempt )?is good|\bsafety\b", re.I)
_GAIN = re.compile(r"for (-?\d+) yards?", re.I)
_YDS = re.compile(r"for (-?\d+) yards?(?: (gain|loss))?|loss of (\d+) yards?", re.I)
_PEN_YDS = re.compile(r"(\d+)\s+yards?", re.I)
_FOULS = ("false start", "offside", "encroachment", "neutral zone", "delay of game", "holding", "pass interference",
          "face mask", "facemask", "personal foul", "unsportsmanlike", "roughing", "targeting", "illegal formation",
          "illegal shift", "illegal motion", "illegal procedure", "substitution", "ineligible", "intentional grounding",
          "illegal forward pass", "illegal block", "block in the back", "chop block", "clipping", "tripping",
          "horse collar", "horse-collar", "kick catch", "fair catch interference", "running into", "illegal use of hands",
          "too many", "12 men", "sideline", "taunting", "illegal touching", "illegal participation", "leverage",
          "leaping", "hurdling", "illegal snap", "out of bounds")


def _int(v):
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def _is_timeout(e):
    return (str(e.get("home_time_out", "")) == "Yes" or str(e.get("away_time_out", "")) == "Yes"
            or str(e.get("down", "")).strip().upper() == "OTO" or bool(_TIMEOUT.match(str(e.get("play_text") or ""))))


def _as_ncaa(it):
    """A CBS item in the shape ncaa_match scores against."""
    d = {"quarter": it.get("quarter"), "clock": it.get("clock"), "text": it.get("text", ""), "drive_text": "",
         "home_score": None, "away_score": None}
    if it.get("kind") == "play":
        dist = "" if it.get("distance") is None else it["distance"]
        yard = "" if it.get("spot_yard") is None else it["spot_yard"]
        d["drive_text"] = f"{it.get('down')} and {dist} at {it.get('spot_code') or ''}{yard}"
    return d


# ── Roger's same-play test ────────────────────────────────────────────────────

def _yards(text):
    m = _YDS.search(str(text or ""))
    if not m:
        return None
    if m.group(3) is not None:
        return -int(m.group(3))
    n = int(m.group(1))
    return -abs(n) if (m.group(2) or "").lower() == "loss" else n


def _names_match(a, b):
    """A player in common. ESPN writes "#7 D.Scott" or a full name ("Connor Calvert 39 Yd Field Goal"); CBS "D.Scott"."""
    na, nb = M._surnames(a), M._surnames(b)
    if not na and not nb:
        return True
    fa, fb = M._fold(a), M._fold(b)
    return (bool(na & nb) or any(re.search(r"\b%s\b" % re.escape(n), fb) for n in na)
            or any(re.search(r"\b%s\b" % re.escape(n), fa) for n in nb))


# The kind of play, for Roger's "play description are the same". Not ncaa_match._action: that puts fumble and penalty
# ahead of rush, so ESPN "(03:42) A.Barnett III rush right for 18 yards loss ... fumbled" and CBS "A.Barnett rushed for
# -18 yards." read as different plays and the run was added twice (UCF @ Pitt Q4, Sep 12). A fumble or a penalty is
# what happened on the play, not what the play was. A QB scramble is a rush; a spike is an incompletion.
_KIND_RES = [(k, re.compile(p, re.I)) for k, p in (
    ("incomplete", r"\bincomplete\b|\bspikes?\b"), ("intercepted", r"\bintercept"), ("sacked", r"\bsack"),
    ("extra point", r"\bkick attempt\b|\bextra point\b"), ("field goal", r"\bfield goal\b"),
    ("kickoff", r"\bkickoff\b|\bkicks\b"), ("punt", r"\bpunts?\b"), ("kneel", r"\bkneel"), ("complete", r"\bcomplete\b"),
    ("rush", r"\bscrambles?\b|\brush|\bran\b|\brun\b"))]


def _kind(text):
    t = str(text or "")
    return next((k for k, rx in _KIND_RES if rx.search(t)), "")


# One foul, two names. MEASURED Sep 14 2026, Cal @ Syracuse Q4 (4&1 at the CAL 18, 5 yards, no play): ESPN "PENALTY CAL
# Illegal Substitution", CBS "PENALTY on CAL-CAL Defensive Too Many Men on Field" - the play was added a second time.
_FOUL_SAME = {"12 men": "too many", "substitution": "too many", "illegal participation": "too many",
              "facemask": "face mask", "horse-collar": "horse collar"}


def _fouls(text):
    t = M._fold(text)
    return {_FOUL_SAME.get(f, f) for f in _FOULS if f in t}


_NO_YARDS = re.compile(r"\bfor yards?\b", re.I)      # CBS sometimes leaves the number out: "rushed for yards."


def _penalty_clause(text):
    i = text.lower().find("penalty")
    return text[i:] if i >= 0 else ""


def _play_part(text):
    """The play without a penalty written after it - "... for 3 yards ... PENALTY on SAC-S.Key Holding" is a completion,
    and its tacklers / penalized player are not the play's players."""
    i = text.lower().find("penalty")
    return text[:i] if i > 0 else text


def _same_foul(a, b):
    ca, cb = _penalty_clause(a), _penalty_clause(b)
    if not ca or not cb:
        return False
    ya, yb = _PEN_YDS.search(ca), _PEN_YDS.search(cb)
    return bool(_fouls(ca) & _fouls(cb)) and (ya.group(1) if ya else None) == (yb.group(1) if yb else None)


def same_description(a, b):
    """Roger's "play description are the same". The houses word a play differently, so: the same player, the same kind
    of play and the same yards.
    A penalty-only line (no snap) is the same foul with the same penalty yards - ESPN and CBS can name different players
    for one foul (Middle Tennessee Q1 Sep 12: "Blattler" vs "Tripp") - and the other line may be the play that foul wiped
    out (ESPN "PENALTY SACST Holding ... NO PLAY" = CBS "J.Sharman pass complete ... PENALTY on SAC-S.Key Holding 10
    yards accepted. No Play."), so its penalty clause decides.
    CBS sometimes writes no yards ("rushed for yards") - then the yards are not compared (Fresno Q2 Sep 12)."""
    a, b = str(a or ""), str(b or "")
    if _PEN_ONLY.match(a) or _PEN_ONLY.match(b):
        return _same_foul(a, b)
    pa, pb = _play_part(a), _play_part(b)
    if not (_names_match(pa, pb) and _kind(pa) == _kind(pb)):
        return False
    if _NO_YARDS.search(pa) or _NO_YARDS.search(pb):
        return True
    return _yards(pa) == _yards(pb)


def _same_situation(a, b):
    """Two ESPN entries for the SAME snap: same down, distance (1 yard typo) and yard line either side of the 50."""
    da, db = str(a.get("down") or "").strip().upper(), str(b.get("down") or "").strip().upper()
    if da != db:
        return False
    if da.isdigit():
        d1, d2 = _int(a.get("distance")), _int(b.get("distance"))
        if d1 is None or d2 is None or abs(d1 - d2) > DISTANCE_TYPO:
            return False
    f1, f2 = _int(a.get("field_position")), _int(b.get("field_position"))
    return f1 is not None and f2 is not None and abs(f1) == abs(f2)


def same_play(e, it):
    """Is CBS item `it` the ESPN entry `e`? Description + down + distance (1 yard typo allowed) + the same yard line on
    either side of the 50. ESPN rows with a special down (KO / FG / EP / 2PT) carry no down and distance to compare,
    so description + yard line decide."""
    if it.get("kind") != "play" or not same_description(e.get("play_text"), it.get("text")):
        return False
    down = str(e.get("down") or "").strip().upper()
    if down.isdigit():
        if it.get("down") is None or int(down) != int(it["down"]):
            return False
        d1, d2 = _int(e.get("distance")), _int(it.get("distance"))
        if d1 is None or d2 is None or abs(d1 - d2) > DISTANCE_TYPO:
            return False
    fp, y = _int(e.get("field_position")), _int(it.get("spot_yard"))
    return fp is not None and y is not None and abs(fp) == y


def _players(text):
    """The play's own players - not the tacklers or the player a penalty names."""
    t = _play_part(str(text or ""))
    t = re.sub(r"\([^)]*\)", " ", t)                                  # ESPN "(#33 D.Holmes Jr.)", "(06:17)"
    t = re.split(r"\btackled by\b|\bpushed out of bounds by\b|\bforced by\b", t, flags=re.I)[0]
    return t


def _share_player(a, b):
    return _names_match(_players(a), _players(b))


def same_snap(e, it):
    """Roger, Sep 14 2026: "Has to be a way for us to not add dups". Beyond his same-play test, the SAME snap written two
    ways by two scorers is still one play. MEASURED on Sep 12's adds (every one judged against ESPN + NCAA):
      same situation   same player, same down, distance within 1, yard line within 1 - the yards gained or even the kind of
                       play differ: Akron Q3 Gant CBS 1&10@30 rush 4 / ESPN 1&9@29 rush 3; Buffalo @ FIU Q1 Holmes->Morrow
                       CBS 25 yds / ESPN 22 yds at 3&7@28; Texas Tech Q4 CBS "Butler rush 4" / ESPN "pass to Butler" 1&10@22
      same moment      same description, yard line within 1, clock within a few seconds - one source typed the down wrong:
                       Tulane Q2 2:27 Lucas 16-yd run CBS 3rd & 10 / ESPN 1st & 10
    Kept (real plays ESPN lacks, checked): Nebraska Q1 5:34 Ridley sack, USC Q3 5:02 missed FG, LSU Q3 10:15 blocked FG."""
    if it.get("kind") != "play":
        return False
    fp, y = _int(e.get("field_position")), _int(it.get("spot_yard"))
    if fp is None or y is None or abs(abs(fp) - y) > SNAP_YARDS:
        return False
    et, ct = str(e.get("play_text") or ""), str(it.get("text") or "")
    # One foul on one snap, the down typed differently (Buffalo @ FIU Q2 Sep 12: ESPN "PENALTY FIU Pass Interference
    # (#8 R.Gadson) 11 yard ... NO PLAY" 2&10 at the 39; CBS "... PENALTY on FIU-R.Gadson Defensive Pass Interference 11
    # yards accepted. No Play." 4&6 at the 39): same foul, same yards, same penalized player, same yard line.
    if _same_foul(et, ct) and M._surnames(_penalty_clause(et)) & M._surnames(_penalty_clause(ct)):
        return True
    if not _share_player(et, ct):
        return False
    down = str(e.get("down") or "").strip().upper()
    if down.isdigit() and it.get("down") is not None and int(down) == int(it["down"]):
        d1, d2 = _int(e.get("distance")), _int(it.get("distance"))
        if d1 is not None and d2 is not None and abs(d1 - d2) <= DISTANCE_TYPO:
            return True
    ec, cc = M.clock_secs(M.norm_clock(e.get("clock"))), it.get("clock_secs")
    return (ec is not None and cc is not None and abs(ec - cc) <= SAME_CLOCK_SECONDS
            and same_description(et, ct))


def _written_differently(e, it):
    """The line-up set this CBS play against ESPN play e and rejected the pair: when both are the same kind of play by
    the same player they are one snap with different details (Buffalo @ FIU Sep 12: the same drive split 25+2+14 yards
    by CBS and 22+6+13 by ESPN), never a play ESPN is missing."""
    et, ct = str(e.get("play_text") or ""), str(it.get("text") or "")
    if _PEN_ONLY.match(et) or _PEN_ONLY.match(ct):
        return False
    return _kind(_play_part(et)) == _kind(_play_part(ct)) != "" and _share_player(et, ct)


# ── lining up ─────────────────────────────────────────────────────────────────

SERIES_SECONDS = 180          # same_series_snap: the two sources' clocks for one snap are never this far apart


def same_series_snap(e, it):
    """Roger, Sep 18 2026: "Down Distance Yardline as it applies in the series of plays... We dont want the same Down
    Distance and yard line to create dupes because they happen to be the same 6 minutes apart in a game."
    The line-up already proposes pairs in sequence order; this is the content test for such a pair when the two
    scorers wrote the snap so differently that nothing else matches - Syracuse @ Pitt Q4: ESPN "#4 J.Johnson rush
    middle for 10 yards to the Pitt44 (#17 C.Woodson)" at 5:14, CBS "A.Odom pass to T.Russell, tackled by C.Woodson
    at PITT 44" at 6:22, both 2nd & 9 at the 46 - one snap, and the live check added CBS's as a second play.
    Same down, distance within 1, the same yard line - and clocks inside SERIES_SECONDS, which is what keeps a
    2nd & 9 at the 46 in the first quarter from pairing with one six minutes later. A penalty-only line never uses
    this (it has no snap)."""
    if it.get("kind") != "play" or it.get("down") is None:
        return False
    et, ct = str(e.get("play_text") or ""), str(it.get("text") or "")
    if _PEN_ONLY.match(et) or _PEN_ONLY.match(ct):
        return False
    down = str(e.get("down") or "").strip().upper()
    if not down.isdigit() or int(down) != int(it["down"]):
        return False
    d1, d2 = _int(e.get("distance")), _int(it.get("distance"))
    if d1 is None or d2 is None or abs(d1 - d2) > DISTANCE_TYPO:
        return False
    fp, y = _int(e.get("field_position")), _int(it.get("spot_yard"))
    if fp is None or y is None or abs(abs(fp) - y) > SNAP_YARDS:
        return False
    ec, cc = M.clock_secs(M.norm_clock(e.get("clock"))), it.get("clock_secs")
    return ec is not None and cc is not None and abs(ec - cc) <= SERIES_SECONDS


def _good_pair(e, it):
    """The line-up's pair is really this play: a penalty-only line never pairs with a snapped play (Middle Tennessee,
    Sep 12), content strong enough, clocks not far apart - or Roger's same-play test, whatever the clocks say."""
    text = str(e.get("play_text") or "")
    if bool(_PEN_ONLY.match(text)) != bool(_PEN_ONLY.match(it.get("text", ""))):
        return same_play(e, it)                  # only the play that very foul wiped out
    if same_play(e, it) or same_series_snap(e, it):
        return True
    if M.score_pair(e, _as_ncaa(it)) < MIN_PAIR_SCORE:
        return False
    mine, theirs = M.clock_secs(M.norm_clock(e.get("clock"))), it.get("clock_secs")
    if mine is None or theirs is None:
        return True
    return abs(mine - theirs) <= (60 if _SNAP.match(text) else 240)


_FLOW_SKIP = re.compile(r"penalty|no play|intercept|fumble|punt|kickoff|field goal|touchdown|safety|timeout|kneel|"
                        r"end of|two.minute|spike", re.I)


def _down_after(play):
    """The down the next snap should have after `play` (Roger: "it should follow the flow ... If its 2nd down on one and
    the next is 1st its probably a First Down through the play"), or None when the play can't say."""
    if _FLOW_SKIP.search(str(play.get("play_text") or "")):
        return None
    d, dist, gain = _int(play.get("down")), _int(play.get("distance")), _int(play.get("gain"))
    if None in (d, dist, gain) or not 1 <= d <= 4:
        return None
    if gain >= dist or "1st down" in str(play.get("play_text") or "").lower():
        return 1
    return d + 1 if d < 4 else None


def _flow_sides_with_espn(rows, i, new_down, new_values=None):
    """True when the plays around row i follow ESPN's down and not the new one - then ESPN keeps it.
    MEASURED Sep 14 2026 (measure_down_flow.py) on the 20 downs NCAA + CBS agreed on against ESPN (62 games): the flow
    followed NCAA + CBS on 9 (SMU's Bradley 4th down among them), ESPN on 4 (Texas A&M Q3 13:09, UMass Q2 13:06 and
    12:51, Michigan St Q2 12:07), split or could not tell on 7."""
    row = rows[i]
    q, poss = row.get("quarter"), row.get("possession")
    scrim = [k for k, r in enumerate(rows) if r.get("quarter") == q and str(r.get("down") or "").strip() in ("1", "2", "3", "4")
             and not _is_timeout(r)]
    if i not in scrim:
        return False
    pos = scrim.index(i)
    espn_ok = new_ok = False
    espn_bad = new_bad = False
    if pos > 0 and rows[scrim[pos - 1]].get("possession") == poss:
        want = _down_after(rows[scrim[pos - 1]])
        if want is not None:
            espn_ok, espn_bad = _int(row.get("down")) == want, _int(row.get("down")) != want
            new_ok, new_bad = _int(new_down) == want, _int(new_down) != want
    if pos + 1 < len(scrim) and rows[scrim[pos + 1]].get("possession") == poss:
        nxt = _int(rows[scrim[pos + 1]].get("down"))
        a, b = _down_after(row), _down_after(dict(row, **dict(new_values or {}, down=new_down)))
        if a is not None and b is not None and a != b:
            espn_ok, espn_bad = espn_ok or nxt == a, espn_bad or nxt != a
            new_ok, new_bad = new_ok or nxt == b, new_bad or nxt != b
    return espn_ok and not espn_bad and new_bad and not new_ok


def _cbs_spot(it, row, home_name, away_name):
    """CBS's spot as the row's field position (own side negative), or None."""
    y = _int(it.get("spot_yard"))
    if y is None:
        return None
    if y == 50:
        return -50
    side = "home" if row.get("possession") == home_name else ("away" if row.get("possession") == away_name else None)
    if not side or not it.get("spot_side"):
        return None
    return -y if it["spot_side"] == side else y


def _fits(work, i, secs):
    """A clock for row i that keeps the quarter's clocks running down (same rule as the NCAA check's clock step)."""
    q = work[i].get("quarter")
    before = [M.clock_secs(x.get("clock")) for x in work[:i] if x.get("quarter") == q]
    after = [M.clock_secs(x.get("clock")) for x in work[i + 1:] if x.get("quarter") == q]
    hi = before[-1] if before and before[-1] is not None else 15 * 60
    lo = after[0] if after and after[0] is not None else 0
    return lo <= secs <= hi and secs != M.clock_secs(work[i].get("clock"))


def _trusted_bound(work, pairs, start, q, above):
    """The clock of the nearest PAIRED play outside a stuck run, walking up (above=True) or down from `start` - the
    nearest neighbour that can fence the run in. Seconds; the quarter's edge when nothing qualifies.

    ⚠ WHY NOT THE ROW NEXT DOOR (Syracuse @ Pitt Q4, Sep 17 2026, Roger watching live): five plays sat at (02:00),
    CBS had them at 3:08 / 3:03 / 2:56 / 2:56 / 2:15, and _fits rejected every one because the row above the run was
    ESPN's kickoff stamped 2:07 - wrong itself (CBS: 3:09), and never paired. A neighbour nobody vouches for cannot
    fence the run in, so a row with no CBS pair is skipped, as are timeouts and admin lines.
    ⚠ BUT A PAIRED ROW FENCES WITH ITS OWN CLOCK, whether or not CBS's clock for it agrees. MEASURED (Sep 12 corpus)
    when this also skipped paired rows CBS disagreed with by 5+ s: FIU @ Buffalo Q2 climbed a five-play run to
    11:26-10:03 over a punt ESPN has at 7:36, and Miami (OH) put a kickoff at 9:37 above the extra point at 9:31 -
    two games made worse. ESPN's clock on a play CBS vouches for is that play's own claim, and it stands."""
    step = -1 if above else 1
    i = start + step
    while 0 <= i < len(work):
        e = work[i]
        if e.get("quarter") != q:
            break
        s = M.clock_secs(M.norm_clock(e.get("clock")))
        if (pairs.get(i) is not None and s is not None
                and not M.is_admin_line(e.get("play_text", "")) and not _is_timeout(e)):
            return s
        i += step
    return 15 * 60 if above else 0


def _stuck_runs(work, min_len=STUCK_RUN):
    """Runs of rows the "Clock stuck" QC error fires on (espn_fetcher._qc_flag_entries): the same clock on consecutive
    rows of one quarter, the rows after the first not a kickoff / try / officials timeout. [row indices]"""
    runs, start = [], 0
    for i in range(1, len(work) + 1):
        if i < len(work):
            c, p = work[i], work[i - 1]
            if (c.get("clock") == p.get("clock") and c.get("quarter") == p.get("quarter")
                    and str(c.get("down", "")) not in ("KO", "EP", "2PT", "OTO")):
                continue
        if i - start >= min_len:
            runs.append(list(range(start, i)))
        start = i
    return runs


def _ncaa_clocks(work, rows, pbp):
    """{row: NCAA clock seconds} for rows CBS could not give a clock - NCAA is the next source in Roger's order."""
    plays = [p for p in (pbp or {}).get("plays") or [] if p.get("text")]
    want = set(rows)
    out = {}
    if not plays or not want:
        return out
    for q in sorted({M._quarter(work[i].get("quarter")) for i in want}):
        mine = [(i, e) for i, e in enumerate(work) if M._quarter(e.get("quarter")) == q
                and not M.is_admin_line(e.get("play_text", ""))]
        theirs = [t for t in plays if M._quarter(t.get("quarter")) == q and not M.is_admin_line(t.get("text", ""))]
        for a, b in ncaa_check._aligned([e for _i, e in mine], theirs):
            if a is None or b is None:
                continue
            i = mine[a][0]
            if i in want and ncaa_check._good_pair(work[i], theirs[b], "guess"):
                secs = ncaa_check._ncaa_secs(theirs[b])
                if secs is not None:
                    out[i] = secs
    return out


def _added_key(q, text):
    body = re.sub(r"\s+", " ", M._fold(text)).strip()
    return "gap:cbs:%s:%s" % (q, hashlib.sha1(body.encode("utf-8")).hexdigest()[:12])


# ── the check ─────────────────────────────────────────────────────────────────

# The check is a pure function of what goes in. Roger, Sep 14 2026: "We need to have everything done before THursday".
# MEASURED (measure_poll_cost.py): one poll of a finished game took 1.2-3.3 s cold with the CBS check (NCAA alone
# 0.5-1.3 s), and the live poller fetches every open game one after another every ~10 s. ESPN, CBS (20 s cache) and
# NCAA (45 s cache) are unchanged on most polls, so the last answer per game is kept and reused while nothing changed.
_MEMO = {}
_MEMO_MAX = 300


def verify_entries(entries, backup, home_name, away_name, clock_src=None, ncaa_pbp=None, cbs_game_id=""):
    """Check ESPN's entries against CBS IN PLACE. Returns a summary. All the work is done on a copy and written back
    at the very end, so an error part-way leaves ESPN's entries exactly as they were (the caller catches it)."""
    clock_src = clock_src or {}
    memo_key, memo_sig = (str(cbs_game_id) if cbs_game_id else None), None
    if memo_key:
        memo_sig = hashlib.sha1(json.dumps([entries, (backup or {}).get("items"), (ncaa_pbp or {}).get("plays"),
                                            clock_src, home_name, away_name], sort_keys=True, default=str)
                                .encode("utf-8")).hexdigest()
        hit = _MEMO.get(memo_key)
        if hit and hit[0] == memo_sig:
            entries[:] = copy.deepcopy(hit[1])
            return copy.deepcopy(hit[2])
    plays = [it for it in ((backup or {}).get("items") or []) if it.get("text") and it.get("kind") in PLAY_KINDS]
    summary = {"available": bool(plays), "source": "cbs", "cbs_game_id": str(cbs_game_id or ""), "verified": 0,
               "fixed": 0, "added": 0, "unverified": 0, "superseded": 0, "skipped_scoring_adds": 0,
               "skipped_duplicate_adds": 0, "skipped_other_adds": 0, "rejected_pairs": 0,
               "clock_fixes": {"cbs": 0, "ncaa": 0}, "stuck_runs": 0, "stuck_confirmed": 0, "out_of_order_verified": 0, "skipped_penalty_only_adds": 0, "duplicate_reasons": {},
               "review_count": 0,
               "examples": []}
    if not plays or not entries:
        return summary
    work = copy.deepcopy(entries)
    for e in work:
        e["ncaa_status"] = ""
        e["ncaa_changes"] = []

    quarters = sorted({M._quarter(e.get("quarter")) for e in work} - {0})
    last_q = max(quarters) if quarters else 0
    pairs, to_add = {}, []
    for q in quarters:
        mine = [(i, e) for i, e in enumerate(work) if M._quarter(e.get("quarter")) == q
                and not M.is_admin_line(e.get("play_text", "")) and not _is_timeout(e)]
        theirs = [it for it in plays if M._quarter(it.get("quarter")) == q]
        events, last = [], None
        for a, b in ncaa_check._aligned([e for _i, e in mine], [_as_ncaa(it) for it in theirs]):
            if a is not None:
                i = mine[a][0]
                if b is not None and _good_pair(work[i], theirs[b]):
                    pairs[i] = theirs[b]
                    events.append(("pair", i))
                else:
                    if b is not None:
                        summary["rejected_pairs"] += 1
                        events.append(("cbs", (last, theirs[b], i)))
                    events.append(("espn", i))
                last = i
            else:
                events.append(("cbs", (last, theirs[b], None)))
        # Between two plays both sources have, CBS can only be MISSING-FROM-ESPN plays when it has MORE plays there
        # than ESPN. Same number = the same snaps written differently. MEASURED Buffalo @ FIU Q2 (Sep 12): ESPN and NCAA
        # have four Shelton runs (6, 0, 1, 17 yards) where CBS has Shelton 6, 14, -1 and a Holmes scramble 5 - four
        # snaps each; the scramble was being added. Nebraska's missing Ridley sack sits alone between two lined-up plays.
        covered, seg = set(), []
        for k, ev in enumerate(events + [("pair", None)]):
            if ev[0] != "pair":
                seg.append(k)
                continue
            n_cbs = sum(1 for j in seg if events[j][0] == "cbs")
            if n_cbs and n_cbs <= sum(1 for j in seg if events[j][0] == "espn"):
                covered.update(j for j in seg if events[j][0] == "cbs")
            seg = []
        # Added only once ESPN has moved past the play (3 lined-up plays after it, or a finished quarter) - it may
        # simply not have reached ESPN yet (same pacing rule as the NCAA check).
        for k, ev in enumerate(events):
            if ev[0] == "cbs" and (q < last_q or sum(1 for x in events[k + 1:] if x[0] == "pair") >= 3):
                to_add.append((q, ev[1][0], ev[1][1], ev[1][2], k in covered))

    # Down (and, once Roger sets NOTICEABLE_YARDS, distance / spot): ESPN loses only when CBS AND NCAA both say another
    # value and agree on it. NCAA's value is what its own check would set - run on a copy, nothing of it is kept.
    # MEASURED (measure_cbs_ncaa_agree.py, 62 Sep 12 games): 79 cells; every answer-key case sided with NCAA + CBS
    # (SMU Perez 2&27 -> 2&4, Bradley 3&10 -> 4&4, 9:51 penalty 1&10 at 30 -> 1&20 at 15; Air Force spot 20 -> 15).
    two_source = {}
    if ncaa_pbp and pairs:
        ncopy = copy.deepcopy(entries)
        refs = list(ncopy)
        ncaa_check.verify_entries(ncopy, ncaa_pbp, home_name, away_name)
        for i, it in pairs.items():
            row = work[i]
            if not str(row.get("down") or "").strip().isdigit() or it.get("kind") != "play":
                continue
            for c in refs[i].get("ncaa_changes") or []:
                f, new = c.get("field"), c.get("new")
                if f == "down" and str(it.get("down")) == str(new) and str(new) != str(row.get("down")).strip():
                    two_source.setdefault(i, []).append(("down", str(new)))
                elif f in ("distance", "field_position") and NOTICEABLE_YARDS is not None:
                    cbs = it.get("distance") if f == "distance" else _cbs_spot(it, row, home_name, away_name)
                    if (_int(cbs) is not None and _int(cbs) == _int(new)
                            and abs((_int(row.get(f)) or 0) - _int(new)) >= NOTICEABLE_YARDS):
                        two_source.setdefault(i, []).append((f, _int(new)))

    def change(i, field, new, why):
        e = work[i]
        e["ncaa_changes"].append({"field": field, "old": e.get(field), "new": new, "why": why})
        if len(summary["examples"]) < 8:
            summary["examples"].append("Q%s %s %s: %s -> %s (%s)" % (e.get("quarter"), e.get("clock"), field,
                                                                   e.get(field), new, why))
        e[field] = new

    # Row by row in game order, so the flow is judged against plays already fixed. MEASURED Sep 14 2026 (UMass Q2):
    # judged against ESPN's 13:39 "2&4", the 6-yard gain looked like a first down and ESPN's 1st down was kept - but
    # CBS + NCAA make 13:39 "2&8", and then their 3rd & 2 at 13:06 and 4th & 1 at 12:51 follow.
    # When the flow keeps ESPN's down, NOTHING on that row changes: a kept down with the other sources' distance made
    # "1st & 2" (UMass 13:06), "2nd & 9" (Texas A&M Q3 13:09), "4th & 10" (Michigan St Q2 12:07).
    for i, fixes in sorted(two_source.items()):
        values = dict(fixes)
        down = values.get("down")
        if down is not None and _flow_sides_with_espn(work, i, down, values):
            summary["two_source_kept_by_flow"] = summary.get("two_source_kept_by_flow", 0) + 1
            continue
        for f, new in fixes:
            change(i, f, new, "CBS + NCAA")
            summary["two_source_fixes"] = summary.get("two_source_fixes", 0) + 1

    # Clocks: ESPN -> CBS -> NCAA -> guess, only where ESPN's clock is messed up (rows first - indices still ESPN's;
    # adds last). Messed up = a GUESS (ESPN's clock was missing, the 3rd play in a row on one clock, or did not fit -
    # espn_fetcher.apply_text_snap_clocks) or a STUCK run. Roger, Sep 14 2026: "If there is a stuck Clock, no Clock, A
    # Clock that is Crazy off then We Check CBS" / "If we get a stuck Clock error we check CBS to See if the clock is
    # stuck for real". A stuck run is checked whatever its clocks came from - typed snaps too (Buffalo @ FIU Sep 12:
    # "(06:05)" typed on 4 plays). CBS within a few seconds on every play of the run = stuck for real, left alone.
    guessed = {i for i, e in enumerate(work) if clock_src.get(str(e.get("espn_play_id"))) == "guess"}
    stuck, stuck_blocks = set(), []
    for run in _stuck_runs(work):
        summary["stuck_runs"] += 1
        rows = [i for i in run if not M.is_admin_line(work[i].get("play_text", "")) and not _is_timeout(work[i])]
        at = M.clock_secs(M.norm_clock(work[run[0]].get("clock")))
        cbs_clocks = [pairs[i]["clock_secs"] for i in rows if i in pairs and pairs[i].get("clock_secs") is not None]
        if not cbs_clocks:
            continue
        if at is not None and all(abs(c - at) <= SAME_CLOCK_SECONDS for c in cbs_clocks):
            summary["stuck_confirmed"] += 1
            continue
        stuck.update(rows)
        stuck_blocks.append(rows)
    todo = [i for i in sorted(guessed | stuck)
            if not M.is_admin_line(work[i].get("play_text", "")) and not _is_timeout(work[i])
            and str(work[i].get("down") or "").strip().upper() not in _NO_CLOCK_DOWNS]
    done = set()
    # Forward, then backward: every play of a stuck run sits on one clock, so a CBS clock only fits once the play
    # beside it has moved - forward when the stuck clock is too low, backward when it is too high.
    for order in (todo, todo[::-1]):
        for i in order:
            it = pairs.get(i)
            if i in done or it is None or it.get("clock_secs") is None:
                continue
            if it["clock_secs"] == M.clock_secs(M.norm_clock(work[i].get("clock"))):
                done.add(i)                          # CBS agrees with the clock the play already has
            elif _fits(work, i, it["clock_secs"]):
                change(i, "clock", "%d:%02d" % divmod(it["clock_secs"], 60),
                       "CBS clock (stuck clock)" if i in stuck else "CBS clock")
                summary["clock_fixes"]["cbs"] += 1
                done.add(i)
    # A stuck run the row-by-row passes could not move at all is taken AS A BLOCK: CBS's clocks for it must run down
    # and must sit between the nearest clocks outside the run that both sources agree on (_trusted_bound). Roger,
    # Sep 17 2026, after Pitt's five (02:00) boards: "the Errors on the clock should be auto fixed by the backup source".
    # Only a run the stuck check already caught - a clock a few seconds off is not an error and stays ESPN's.
    for rows_ in stuck_blocks:
        need = [i for i in rows_ if i in todo and i not in done]
        # ⚠ THE WHOLE RUN OR NOTHING. MEASURED (Sep 12 corpus): FIU @ Buffalo Q1's run of four had CBS pairs for two;
        # moving those two to 8:20 / 7:41 left the unpaired two sitting at 6:05 above them - a run half moved is
        # worse than a run left stuck. A row of the run nobody can clock refuses the block for all of them.
        if len(need) < 2 or any(i not in pairs or pairs[i].get("clock_secs") is None for i in need):
            continue
        block = need
        secs = [pairs[i]["clock_secs"] for i in block]
        if any(b > a for a, b in zip(secs, secs[1:])):
            continue                                 # CBS's own clocks do not run down - nothing to trust here
        q = work[block[0]].get("quarter")
        hi = _trusted_bound(work, pairs, rows_[0], q, above=True)
        lo = _trusted_bound(work, pairs, rows_[-1], q, above=False)
        if not (lo <= secs[-1] and secs[0] <= hi):
            continue
        for i, s in zip(block, secs):
            if s != M.clock_secs(M.norm_clock(work[i].get("clock"))):
                change(i, "clock", "%d:%02d" % divmod(s, 60), "CBS clock (stuck clock)")
                summary["clock_fixes"]["cbs"] += 1
            done.add(i)
        summary["stuck_blocks"] = summary.get("stuck_blocks", 0) + 1
    # ⚠ A RUN OF TYPED CLOCKS THAT ARE ALL OFF THE SAME WAY takes CBS's clocks - Roger, Sep 18 2026, on the measured
    # numbers: "the clock that makes sense wins". MEASURED on 70 games / 9,960 paired plays with a typed snap: the
    # two sources sit within 7 s on 90% of plays and 40 s on 99%; only 1.3% differ by 30+ s, and a LONE big gap can be
    # CBS's fault (period-boundary junk: ESPN 0:41 vs CBS 15:00). What is never CBS's fault is a STRETCH of plays all
    # late by a minute in the same direction (Syracuse @ Pitt Q4: 5:14 / 4:15 / 3:46 / 3:13 / 3:09 / 3:09 / 3:09 where
    # CBS has 6:22 / 5:44 / 5:25 / 4:58 / 4:57 / 4:28 / 3:50 - ESPN's version ends in three snaps on one second).
    # So: three or more consecutive paired plays, every typed clock 30+ s from CBS's and all the same way, CBS's
    # clocks running down and sitting between the paired plays either side of the run (which by construction agree
    # with CBS within 30 s - they ended the run). Timeouts and admin lines are transparent; an unpaired play or a
    # play the two sources agree on ends the run. One disagreement alone stays ESPN's.
    def _flush_off_run(run):
        if len(run) < OFF_RUN_MIN:
            return
        secs = [pairs[i]["clock_secs"] for i in run]
        if any(b > a for a, b in zip(secs, secs[1:])):
            return
        q = work[run[0]].get("quarter")
        hi = _trusted_bound(work, pairs, run[0], q, above=True)
        lo = _trusted_bound(work, pairs, run[-1], q, above=False)
        if not (lo <= secs[-1] and secs[0] <= hi):
            return
        for i, s in zip(run, secs):
            change(i, "clock", "%d:%02d" % divmod(s, 60), "CBS clock (typed clocks off as a run)")
            summary["clock_fixes"]["cbs"] += 1
            done.add(i)
        summary["off_runs"] = summary.get("off_runs", 0) + 1

    off_run, off_dir = [], 0
    for i, e in enumerate(work):
        if M.is_admin_line(e.get("play_text", "")) or _is_timeout(e):
            continue                                     # transparent: a timeout inside a late stretch is still one stretch
        it = pairs.get(i)
        s = M.clock_secs(M.norm_clock(e.get("clock")))
        direction = 0
        if (it is not None and it.get("clock_secs") is not None and s is not None and i not in done
                and _SNAP.match(str(e.get("play_text") or ""))
                and str(e.get("down") or "").strip().upper() not in _NO_CLOCK_DOWNS):
            d = it["clock_secs"] - s
            direction = 1 if d > OFF_RUN_SECONDS else (-1 if d < -OFF_RUN_SECONDS else 0)
        if direction and off_run and direction == off_dir and work[off_run[-1]].get("quarter") == e.get("quarter"):
            off_run.append(i)
            continue
        _flush_off_run(off_run)
        off_run, off_dir = ([i], direction) if direction else ([], 0)
    _flush_off_run(off_run)
    need_ncaa = [i for i in todo if i not in done]
    if need_ncaa and ncaa_pbp:
        for i, secs in sorted(_ncaa_clocks(work, need_ncaa, ncaa_pbp).items()):
            if _fits(work, i, secs):
                change(i, "clock", "%d:%02d" % divmod(secs, 60), "NCAA clock")
                summary["clock_fixes"]["ncaa"] += 1

    # A play the line-up could not pair because ESPN filed it out of order (SMU Q2 Sep 12: ESPN "(05:39) J.Fisher rush
    # 0 yards" sits before a Fisher run CBS has two plays earlier) is still a play CBS has - Roger's same-play test -
    # but only against a CBS play the line-up LEFT OVER, and each CBS play vouches for one ESPN row. A crew's replaced
    # entry must stay unverified when CBS's one copy of that play is already lined up with the corrected entry.
    used = {id(it) for it in pairs.values()}
    for i, e in enumerate(work):
        if M.is_admin_line(e.get("play_text", "")) or _is_timeout(e):
            continue
        in_cbs = i in pairs
        if not in_cbs:
            spare = next((it for it in plays if id(it) not in used
                          and M._quarter(it.get("quarter")) == M._quarter(e.get("quarter")) and same_play(e, it)), None)
            if spare is not None:
                used.add(id(spare))
                in_cbs = True
                summary["out_of_order_verified"] += 1
        if e["ncaa_changes"]:
            e["ncaa_status"] = "fixed"
        else:
            e["ncaa_status"] = "verified" if in_cbs else "unverified"
        summary[e["ncaa_status"]] += 1

    # Superseded crew entries (Roger, Sep 14 2026: "we only keep the corrected version"): ESPN keeps an entry the crew
    # replaced as its own play under the SAME sequence number. CBS decides, the way NCAA did (ncaa_check): a play CBS
    # did not line up is the leftover when CBS lined up another play in its group by the same player.
    groups = {}
    for i, e in enumerate(work):
        text = str(e.get("play_text") or "")
        low = text.lower()
        if (e.get("espn_seq") is None or not e.get("espn_play_id") or M.is_admin_line(text) or _is_timeout(e)
                or low.startswith("end of") or "kickoff" in low or "extra point" in low
                or str(e.get("down", "")).strip().upper() in _NO_CLOCK_DOWNS):
            continue
        groups.setdefault((M._quarter(e.get("quarter")), str(e.get("espn_seq"))), []).append(i)
    for idxs in groups.values():
        if len({str(work[i].get("espn_play_id")) for i in idxs}) < 2:
            continue
        matched = [i for i in idxs if work[i].get("ncaa_status") in ("verified", "fixed")]
        for i in idxs:
            e = work[i]
            if e.get("ncaa_status") != "unverified":
                continue
            names = M._surnames(e.get("play_text"))
            # ⚠ NOT "the same down/distance/spot" (tried Sep 14 2026): a crew's first entry often has the WRONG down -
            # SMU Q2 "(07:09) 4&4 incomplete ... TURNOVER ON DOWNS" replaced by the 1&10 play - and that guard left
            # SMU, Penn State @ Temple, Buffalo @ FIU and 4 more games with their leftovers on the board. A real play
            # that only shares a sequence number (SMU's 4&1 Fisher run) is kept by the out-of-order rule above: CBS has it.
            keep = next((k for k in matched if names & M._surnames(work[k].get("play_text"))), None)
            if keep is None:
                continue
            e["ncaa_status"] = "superseded"
            e["superseded_by"] = str(work[keep].get("espn_play_id"))
            summary["unverified"] -= 1
            summary["superseded"] += 1

    # Which CBS team id is which team, learned from lined-up plays (CBS writes its own team ids).
    poss_votes = {}
    for i, it in pairs.items():
        if it.get("team_id") and work[i].get("possession") in (home_name, away_name):
            poss_votes.setdefault(it["team_id"], Counter())[work[i]["possession"]] += 1

    pair_by_row = {id(work[i]): it for i, it in pairs.items()}      # the clock step after the adds - rows shift when they go in
    inserts, seen_keys = [], set()
    reasons = Counter()
    for q, after, it, rejected_with, gap_covered in to_add:
        text = str(it.get("text") or "")
        if it.get("kind") != "play" or it.get("down") is None or it.get("clock_secs") is None:
            summary["skipped_other_adds"] += 1            # kickoffs / tries / a line with no down or clock
            continue
        if _SCORING.search(text):
            summary["skipped_scoring_adds"] += 1          # a score changes every later board - left for review
            continue
        key = _added_key(q, text)
        near = [e for e in work if abs(M._quarter(e.get("quarter")) - q) <= 1 and not _is_timeout(e)]
        why = ("listed twice" if key in seen_keys
               else "same play" if any(same_play(e, it) for e in near)
               else "same snap" if any(same_snap(e, it) for e in near)
               else "ESPN has as many plays there" if gap_covered
               else "rejected pair, same player and kind" if (rejected_with is not None
                                                                and _written_differently(work[rejected_with], it))
               else "")
        if why:
            reasons[why] += 1
            summary["skipped_duplicate_adds"] += 1
            continue
        if _PEN_ONLY.match(text) and not ADD_PENALTY_ONLY:
            summary["skipped_penalty_only_adds"] += 1
            continue
        seen_keys.add(key)
        pos = (after + 1) if after is not None else next(
            (k for k, e in enumerate(work) if M._quarter(e.get("quarter")) == q), len(work))
        inserts.append((pos, q, it, key))
    for pos, q, it, key in sorted(inserts, key=lambda x: (x[0], x[2].get("seq", 0)), reverse=True):
        text = str(it.get("text") or "")
        prev = next((work[k] for k in range(pos - 1, -1, -1) if M._quarter(work[k].get("quarter")) == q), None)
        nxt = next((work[k] for k in range(pos, len(work)) if M._quarter(work[k].get("quarter")) == q), None)
        base = prev or nxt or {}
        # Whose play: between two ESPN plays of one team it is that team's drive. CBS's own team field was wrong on
        # Nebraska's Q1 5:34 Ridley sack (Sep 12: Nebraska for a Bowling Green snap). Otherwise CBS's team, learned from
        # the lined-up plays; otherwise the play before it.
        votes = poss_votes.get(it.get("team_id"))
        if (prev and nxt and prev.get("possession") == nxt.get("possession")
                and prev.get("possession") in (home_name, away_name)):
            poss = prev["possession"]
        elif votes:
            poss = votes.most_common(1)[0][0]
        else:
            poss = base.get("possession", "")
        side = "home" if poss == home_name else ("away" if poss == away_name else None)
        y = _int(it.get("spot_yard"))
        if y is None:
            fp = base.get("field_position", 0)
        elif y == 50:
            fp = -50
        elif side and it.get("spot_side"):
            fp = -y if it["spot_side"] == side else y
        else:
            fp = base.get("field_position", 0)
        g = _GAIN.search(text)
        entry = {
            "home_score": base.get("home_score", 0),
            "away_score": base.get("away_score", 0),
            "clock": it["clock"],
            "quarter": base.get("quarter", q),
            "down": str(it["down"]),
            "distance": it["distance"] if it.get("distance") is not None else 10,
            "gain": 0 if ("no play" in text.lower() or not g) else int(g.group(1)),
            "field_position": fp,
            "possession": poss,
            "run_clock": "No",
            "home_time_out": "No",
            "away_time_out": "No",
            "play_text": text,
            "wallclock": "",
            "espn_play_id": "",
            "espn_seq": None,
            "_forced_key": key,
            "ncaa_status": "added",
            "ncaa_changes": [{"field": "row", "old": "", "new": "added", "why": "CBS has this play, ESPN does not"}],
        }
        work.insert(pos, entry)
        summary["added"] += 1
        if len(summary["examples"]) < 8:
            summary["examples"].append("Q%s %s added from CBS: %s" % (entry["quarter"], entry["clock"], text[:60]))

    # ESPN's own clock that runs BACKWARDS once CBS's missing plays are in (Roger, Sep 15 2026: "the 6:03 Time is what makes
    # it out of order... if a user sees that they are going to assume its the wrong play or wrong time" / "Even if its a 1
    # off today it doesnt mean that it doesnt happen 20 times on Saturday"). apply_text_snap_clocks trusts ESPN's clock when
    # it fits the TYPED snaps around it - it cannot see a play only CBS has. Nebraska Q1 (Sep 12): no snap typed on the
    # Amachree run, ESPN's 6:03 fits 6:12 / 4:39, CBS's 5:34 sack goes in above it -> 6:03 under 5:34 -> CBS's 5:20 (the
    # answer key). ESPN clocks only - a typed snap still outranks everything. MEASURED on Sep 12's 125 games before it went
    # in: exactly that one clock changed, 0 games worse (every_game_check clk0_0915 -> clk1_0915).
    last_clock = {}
    for i, e in enumerate(work):
        if M.is_admin_line(e.get("play_text", "")):
            continue
        q = M._quarter(e.get("quarter"))
        s = M.clock_secs(M.norm_clock(e.get("clock")))
        if s is None or q > 4:
            continue
        if (q in last_clock and s > last_clock[q] + BACKWARDS_MARGIN and not _is_timeout(e)
                and clock_src.get(str(e.get("espn_play_id"))) == "espn"):
            it = pair_by_row.get(id(e))
            if it is not None and it.get("clock_secs") is not None and _fits(work, i, it["clock_secs"]):
                change(i, "clock", "%d:%02d" % divmod(it["clock_secs"], 60), "CBS clock (ESPN clock ran backwards)")
                summary["clock_fixes"]["cbs_backwards"] = summary["clock_fixes"].get("cbs_backwards", 0) + 1
                if e.get("ncaa_status") == "verified":
                    e["ncaa_status"] = "fixed"
                    summary["verified"] -= 1
                    summary["fixed"] += 1
                s = it["clock_secs"]
        last_clock[q] = s

    summary["duplicate_reasons"] = dict(reasons)
    summary["review_count"] = summary["fixed"] + summary["added"]
    if memo_key:
        if len(_MEMO) >= _MEMO_MAX:
            _MEMO.clear()
        _MEMO[memo_key] = (memo_sig, copy.deepcopy(work), copy.deepcopy(summary))
    entries[:] = work
    return summary
