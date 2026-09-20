"""CBS as the BACKUP SOURCE a coach can swap a bad quarter to (Roger, Sep 14 2026).

Roger: "Q1 SCOREBOARDS HAVE OVER A 10% ERROR RATE... WOULD YOU LIKE TO USE THE BACKUP SOURCE?" - each quarter judged on
its own rows, the prompt in the app, and "Yes" makes the backup the primary for THAT quarter ("All in means the backup
becomes the primary for that game" / "Remember the primary backup is CBS"; NCAA only when CBS has no play-by-play).

This builds the items SBENTRY swaps a quarter to - the same "capp-backup-data" items Resolve reads (cbs_live.to_items),
plus the two things CBS does not write as plays:
  scores    CBS writes NO score numbers on a play - only score_type ("Touchdown", "PointAfterTouchdown", "FieldGoal",
            "TwoPointConversion", "Safety"). Roger: "Once a score is tagged on a play it Stays that way until there is
            another score". The team is not on the tag. READ on UTAH vs ARK (50027684): CBS names the SCORING team on the
            try and on the kickoff after a score, and the kicker's team on a field goal. So a touchdown goes to the team
            of the try / kickoff that follows it, a safety to the team that does NOT kick next.
            MEASURED (measure_cbs_primary_fields.py, 69 Sep 12 games): final score exact on 68/69 this way
            (team_in_possession alone 50/69).
  timeouts  CBS writes no timeout lines; every play carries home/away_timeouts_remaining. Roger: "Im not worried about TC
            Timeouts... Yes the TImeouts Left field coud work" - team timeouts only. The field BLIPS (UTAH vs ARK: "2/1" on
            one play then 3/3; "0/0" on a punt), so a drop counts only when the lower number holds for the next
            TIMEOUT_HOLD plays of that half. MEASURED: 372 drops matched an ESPN team timeout, 44 did not, 35 ESPN team
            timeouts had no drop.
Never raises into the play feed: callers get {"available": False, "note": ...}.
"""
import re

import cbs_live

POINTS = {"Touchdown": 6, "PointAfterTouchdown": 1, "FieldGoal": 3, "TwoPointConversion": 2, "Safety": 2}
TIMEOUT_HOLD = 2
_NO_GOOD = re.compile(r"\b(fail|failed|no good|unsuccessful|missed|blocked)\b", re.I)


def _subs(p):
    s = ((p.get("subplays") or {}).get("subplay")) or []
    return [x for x in ([s] if isinstance(s, dict) else s) if isinstance(x, dict)]


def _is_try_or_kickoff(p):
    text = f" {str(p.get('description') or '').lower()} "
    return (p.get("score_type") in ("PointAfterTouchdown", "TwoPointConversion") or " kicks " in text
            or any(x.get("type") in ("Kickoff", "PointAfterTouchdown", "MissedPointAfterTouchdown") for x in _subs(p)))


def scores_after(plays, home_id, away_id):
    """[(home, away)] - the score AFTER each CBS play. home_id / away_id: CBS's team ids for ESPN's home / away."""
    h = a = 0
    out = []
    for k, p in enumerate(plays):
        st = p.get("score_type")
        if st in POINTS and not (st in ("TwoPointConversion", "PointAfterTouchdown")
                                 and _NO_GOOD.search(str(p.get("description") or ""))):
            team = str(p.get("team_in_possession") or "")
            if st in ("Touchdown", "Safety"):
                nxt = next((q for q in plays[k + 1:k + 4] if _is_try_or_kickoff(q)), None)
                if nxt is not None and nxt.get("team_in_possession"):
                    team = str(nxt["team_in_possession"])
                    if st == "Safety":
                        team = away_id if team == home_id else home_id
                elif st == "Safety":
                    team = away_id if team == home_id else home_id
            if team == home_id:
                h += POINTS[st]
            elif team == away_id:
                a += POINTS[st]
        out.append((h, a))
    return out


def _half(p):
    try:
        q = int(p.get("quarter"))
    except (TypeError, ValueError):
        return 0
    return 1 if q <= 2 else (2 if q <= 4 else q)


def team_timeouts(plays):
    """{play index: ["home"/"away", ...]} in CBS's own home/away terms - one entry per timeout, at the first play that
    shows the lower number (the timeout was called before that snap)."""
    out = {}
    for side, key in (("home", "home_timeouts_remaining"), ("away", "away_timeouts_remaining")):
        vals = []
        for p in plays:
            try:
                vals.append(int(p.get(key)))
            except (TypeError, ValueError):
                vals.append(None)
        level = None
        for k, p in enumerate(plays):
            v = vals[k]
            if v is None:
                continue
            if level is None or (k > 0 and _half(plays[k - 1]) != _half(p)):
                level = v
                continue
            ahead = [vals[j] for j in range(k + 1, min(len(plays), k + 1 + TIMEOUT_HOLD)) if _half(plays[j]) == _half(p)]
            if v < level and all(x is not None and x <= v for x in ahead):
                out.setdefault(k, []).extend([side] * (level - v))
                level = v
    return out


def ready_through(plays, status):
    """Quarters CBS has finished: every quarter when CBS says FINAL, otherwise every quarter before the newest play's.
    NOT CHECKED YET: what CBS's status says at halftime - until Q3's first play the half is not offered."""
    qs = []
    for p in plays or []:
        try:
            qs.append(int(p.get("quarter")))
        except (TypeError, ValueError):
            pass
    top = max(qs) if qs else 0
    if str(status or "").strip().upper() == "FINAL":
        return max(4, top)
    return max(0, min(top - 1, 4))


_LABELS = {1: "1st Quarter", 2: "Half", 3: "3rd Quarter", 4: "4th Quarter"}


def build(plays, board, home_code, away_code, swapped=False, espn_game_id=""):
    """The backup items for a CBS game. board: cbs_live.scoreboard() (status + CBS's two team ids)."""
    board = board or {}
    home_id, away_id = str(board.get("home_team_id") or ""), str(board.get("away_team_id") or "")
    if swapped:                                    # CBS lists ESPN's home team as its away team (neutral site)
        home_id, away_id = away_id, home_id
    doc = cbs_live.to_items(plays, home_code, away_code, swapped)
    by_id = {it["cbs_id"]: it for it in doc["items"]}
    score = scores_after(plays, home_id, away_id) if home_id and away_id else [(None, None)] * len(plays)
    stops = team_timeouts(plays)
    # CBS writes team_in_possession "0" on a few plays (MEASURED: 9 of 11,544 on Sep 12, mostly penalty lines). The drive
    # says whose ball it is when the drive's other plays name one team (6 of the 9); otherwise the play before it.
    drive_team = {}
    first_snap_team = {}
    for p in plays:
        t = str(p.get("team_in_possession") or "")
        if t not in ("", "0"):
            drive_team.setdefault(str(p.get("drive_id")), set()).add(t)
            low = f" {str(p.get('description') or '').lower()} "
            if (" kicks " not in low and "intercept" not in low
                    and p.get("score_type") not in ("PointAfterTouchdown", "TwoPointConversion")):
                first_snap_team.setdefault(str(p.get("drive_id")), t)
    items = []
    last_team = ""
    for k, p in enumerate(plays):
        t = str(p.get("team_in_possession") or "")
        if t in ("", "0"):
            one = drive_team.get(str(p.get("drive_id"))) or set()
            p = dict(p, team_in_possession=next(iter(one)) if len(one) == 1 else last_team)
        # An interception: CBS names the team that PICKED IT OFF (MEASURED Sep 14 2026: 84 of 108 interceptions name a
        # team other than their drive's; SMU Q2 2:00 "T.Cleeland pass INTERCEPTED" carries SMU). The board is the passing
        # team's snap (answer key: UC Davis), so the drive's first snap team has the ball.
        if "intercept" in str(p.get("description") or "").lower() and first_snap_team.get(str(p.get("drive_id"))):
            p = dict(p, team_in_possession=first_snap_team[str(p.get("drive_id"))])
        last_team = str(p.get("team_in_possession") or "") or last_team
        clock = cbs_live._norm_clock(p.get("time_remaining"))
        for cbs_side in stops.get(k, []):
            side = ("away" if cbs_side == "home" else "home") if swapped else cbs_side
            code = home_code if side == "home" else away_code
            items.append({"kind": "team_timeout", "timeout_side": side, "quarter": cbs_live._int(p.get("quarter")) or 0,
                          "clock": clock, "clock_secs": _secs(clock), "clock_is_snap": True,
                          "text": f"Timeout {code}" + (f", clock {_mmss(clock)}" if clock else ""),
                          "cbs_id": f"{p.get('id')}:timeout"})
        it = by_id.get(str(p.get("id") or ""))
        if it is None:
            continue                               # to_items left it out (no down on a snap) - counted in "skipped"
        h, a = score[k]
        before = score[k - 1] if k else (0, 0)
        team = str(p.get("team_in_possession") or "")
        items.append(dict(it, clock_is_snap=True, score_home=h, score_away=a,
                          score_home_before=before[0], score_away_before=before[1],
                          score_type=str(p.get("score_type") or ""),
                          team_side="home" if team and team == home_id else ("away" if team and team == away_id else None)))
    for seq, it in enumerate(items, 1):
        it["seq"] = seq
    through = ready_through(plays, board.get("status"))
    return {"format": "capp-backup-data", "version": 1, "source": "cbs", "espn_game_id": str(espn_game_id),
            "ready": {"through": through, "label": "Full Game" if str(board.get("status") or "").upper() == "FINAL"
                      else _LABELS.get(through, "")},
            "has_timeouts": True, "teams_known": bool(home_id and away_id),
            # Sep 19 2026: the timeout classifier trusts the counters only when CBS carried them for this game.
            "counters_known": any(p.get("home_timeouts_remaining") is not None or p.get("away_timeouts_remaining") is not None
                                  for p in plays),
            "items": items, "skipped": doc.get("skipped") or {}, "unparsed": [], "errors": []}


def _secs(clock):
    try:
        m, s = str(clock).split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return None


def _mmss(clock):
    s = _secs(clock)
    return "%02d:%02d" % divmod(s, 60) if s is not None else ""


def for_game(game_date, home_team_id, away_team_id, espn_game_id="", league="cfb"):
    """Find the CBS game for an ESPN game and build its backup items. Never raises.
    league picks CBS's per-league table (see cbs_live.find_game) - an ESPN team id alone does not say which sport."""
    try:
        cg = cbs_live.find_game(game_date, home_team_id, away_team_id, league)
        if not cg.get("available"):
            return {"available": False, "note": cg.get("note") or "No backup game found."}
        pbp = cbs_live.play_by_play(cg["cbs_game_id"])
        if not pbp.get("available"):
            return {"available": False, "note": pbp.get("note") or "The backup source has no play-by-play for this game."}
        out = build(pbp["plays"], cbs_live.scoreboard(cg["cbs_game_id"]), cg["home_code"], cg["away_code"],
                    cg.get("swapped", False), espn_game_id)
        out["available"] = True
        out["cbs_game_id"] = cg["cbs_game_id"]
        return out
    except Exception as e:
        return {"available": False, "note": f"{type(e).__name__}: {e}"}
