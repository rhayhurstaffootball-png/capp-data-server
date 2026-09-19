"""The admin Game Day BOARD: one card per licensed school's game on a date, with what the server knows about it.

Roger, Sep 18 2026 (Houston @ Texas Tech, live): "I want to rebuild the gameday page so I can see All the Gamedays of
Licensed CAPP Users with Mini Game Cards that give us all the stats and I can Click it and it expands and I can see all
the plays Downloading. That way I can Monitor all the games tomorrow without having a bunch of Versions of CAPP open."

board(date, clients, request_stats):
    ESPN's scoreboard for the date (FBS + FCS) -> the game of every licensed, active account whose school is mapped
    below -> a card: kickoff, opponent, TV, ESPN status/score, and - once the server has the game in its plays cache
    (a coach opened it) - the same numbers the checks produce: rows, verified/fixed/added, per-quarter issue share
    (the 10% alert's own definition), feed state, request rate. CBS's play-by-play flag ("enhanced") per game, so a
    game without a backup is visible before it starts.
plays(game_id, league):
    the rows exactly as a keyed client (SBENTRY) receives them, trimmed to what the board shows.

Read-only. Nothing here changes what coaches receive.
"""
import json
import re
import time

import espn_fetcher as F
import ncaa_check

# account username -> (school as shown, ESPN team id). Only licensed + active accounts are shown; the rest are here so
# a flipped flag needs no code change. ESPN ids read from ESPN's own scoreboard (Sep 18-19 2026).
LICENSE_TEAMS = {
    "AirForce1": ("Air Force", "2005"),
    "Byu1": ("BYU", "252"),
    "fresno-state": ("Fresno State", "278"),
    "Houston1": ("Houston", "248"),
    "Maryland1": ("Maryland", "120"),
    "MiddleTennessee1": ("Middle Tennessee", "2393"),
    "Nebraska1": ("Nebraska", "158"),
    "Smu1": ("SMU", "2567"),
    "EasternKentucky1": ("Eastern Kentucky", "2198"),
    "NorthTexas1": ("North Texas", "249"),
    "Oklahoma1": ("Oklahoma", "201"),
    "Uconn1": ("UConn", "41"),
}

_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
_sb_cache = {}          # (date, group) -> (time, events)
_cbs_cache = {}         # espn game id -> (time, {"cbs_id", "enhanced"})
SB_TTL = 30
CBS_TTL = 600


def _events(date):
    out = []
    for grp in (80, 81):
        key = (date, grp)
        hit = _sb_cache.get(key)
        if hit and time.time() - hit[0] < SB_TTL:
            out += hit[1]
            continue
        try:
            r = F._session.get(_SCOREBOARD, params={"dates": date, "limit": 400, "groups": grp}, timeout=20)
            r.raise_for_status()
            evs = r.json().get("events") or []
        except Exception:
            evs = hit[1] if hit else []
        _sb_cache[key] = (time.time(), evs)
        out += evs
    return out


def _cbs(date, home_id, away_id, gid):
    hit = _cbs_cache.get(gid)
    if hit and time.time() - hit[0] < CBS_TTL:
        return hit[1]
    info = {"cbs_id": "", "enhanced": ""}
    try:
        import cbs_live
        found = cbs_live.find_game(date, str(home_id), str(away_id))
        cbs_id = (found or {}).get("cbs_game_id") or (found or {}).get("id") or (found if isinstance(found, str) else "")
        if cbs_id:
            info["cbs_id"] = str(cbs_id)
            # the raw scoreboard body: cbs_live.scoreboard() trims it to status/quarter/team ids, and "enhanced" -
            # "Yes" = CBS will carry play-by-play (measured 70/70 on the Sep 12 corpus) - is what the board wants
            status, body = cbs_live._call(cbs_live.BOARD_URL.format(str(cbs_id)))
            m = re.search(r'"enhanced":\s*"?(\w+)', json.dumps(body)) if status == 200 and body else None
            info["enhanced"] = m.group(1) if m else ""
    except Exception:
        pass
    _cbs_cache[gid] = (time.time(), info)
    return info


def _issue(r):
    return (bool(r.get("qc_issue")) or bool(r.get("ncaa_changes")) or r.get("ncaa_status") == "added"
            or r.get("clock_source") == "guess")


def quarter_shares(entries):
    per = {}
    for r in entries:
        if r.get("ncaa_status") == "superseded" or ncaa_check.after_play_penalty(r):
            continue
        q = str(r.get("quarter"))
        t = per.setdefault(q, {"rows": 0, "issues": 0})
        t["rows"] += 1
        t["issues"] += 1 if _issue(r) else 0
    for q, t in per.items():
        t["share"] = round(t["issues"] / t["rows"], 3) if t["rows"] else 0.0
    return dict(sorted(per.items(), key=lambda kv: kv[0]))


def _cached_payload(gid):
    with F._lock:
        return F._plays_cache.get(str(gid))


def board(date, clients, request_stats=None):
    """One card per licensed account with a game on `date` (YYYYMMDD)."""
    request_stats = request_stats or {}
    events = _events(date)
    by_team = {}
    for ev in events:
        try:
            comp = ev["competitions"][0]
            teams = {c["homeAway"]: c for c in comp["competitors"]}
            for side in ("home", "away"):
                by_team[str(teams[side]["team"].get("id"))] = (ev, comp, teams, side)
        except Exception:
            continue
    monitor = {r["game_id"]: r for r in F.get_game_monitor_rows()}
    cards, unmapped = [], []
    for c in clients or []:
        if not (c.get("licensed") and c.get("active")):
            continue
        user = str(c.get("username") or "")
        school, team_id = LICENSE_TEAMS.get(user, (user, ""))
        if not team_id:
            unmapped.append(user)
            continue
        hit = by_team.get(team_id)
        card = {"username": user, "school": school, "team_id": team_id, "game_id": "", "has_game": bool(hit)}
        if not hit:
            cards.append(card)
            continue
        ev, comp, teams, side = hit
        me, opp = teams[side], teams["away" if side == "home" else "home"]
        st = (ev.get("status") or {}).get("type") or {}
        gid = str(ev.get("id"))
        card.update({
            "game_id": gid, "home_away": side, "opponent": opp["team"].get("displayName", ""),
            "home_name": teams["home"]["team"].get("displayName", ""), "away_name": teams["away"]["team"].get("displayName", ""),
            "kickoff": ev.get("date", ""), "tv": ", ".join(b.get("names", [""])[0] for b in comp.get("broadcasts", []) if b.get("names")),
            "espn_state": st.get("state", ""), "espn_detail": st.get("shortDetail", "") or st.get("detail", ""),
            "period": (ev.get("status") or {}).get("period", 0), "clock": (ev.get("status") or {}).get("displayClock", ""),
            "home_score": teams["home"].get("score", ""), "away_score": teams["away"].get("score", ""),
            "league": "cfb",
        })
        # cbs_live.find_game wants ESPN's UTC kickoff stamp (it tries that date and the day before), not YYYYMMDD
        card.update(_cbs(ev.get("date") or date, teams["home"]["team"].get("id"), teams["away"]["team"].get("id"), gid))
        mon = monitor.get(gid)
        payload = _cached_payload(gid)
        if mon:
            card.update({k: mon.get(k) for k in ("status", "plays_count", "auto_fixed_count", "auto_fixed_examples",
                                                  "qc_issue_count", "qc_examples", "feed_state", "seconds_since_new_play",
                                                  "age_seconds", "fetched_at")})
        if payload:
            entries = payload.get("entries") or []
            nc = (payload.get("qc_summary") or {}).get("ncaa_check") or {}
            card["check"] = {k: nc.get(k) for k in ("source", "verified", "fixed", "added", "unverified", "clock_fixes",
                                                     "stuck_blocks", "off_runs", "review_count")}
            card["quarters"] = quarter_shares(entries)
            card["tracked"] = True
        else:
            card["tracked"] = False
        rs = request_stats.get(gid) or {}
        card["requests_last_300s"] = rs.get("requests_last_300s", 0)
        card["requests_last_60s"] = rs.get("requests_last_60s", 0)
        card["plays_requests"] = rs.get("plays_requests", 0)
        cards.append(card)
    order = {"in": 0, "pre": 1, "post": 2}
    cards.sort(key=lambda c: (not c["has_game"], order.get(c.get("espn_state"), 3), c.get("kickoff", ""), c["school"]))
    return {"date": date, "generated_at": time.time(), "cards": cards, "unmapped": unmapped}


def plays(game_id, league="cfb"):
    """The rows a keyed client receives, trimmed for the board's expanded view."""
    payload = F.get_game_plays(str(game_id), league=league, force_refresh=False)
    payload = ncaa_check.for_keyed_client(payload)
    entries = payload.get("entries") or []
    rows = []
    for i, e in enumerate(entries, 1):
        ch = e.get("ncaa_changes") or []
        rows.append({
            "n": i, "key": str(e.get("entry_key") or ""), "quarter": e.get("quarter"), "clock": e.get("clock"),
            "home_score": e.get("home_score"), "away_score": e.get("away_score"), "down": e.get("down"),
            "distance": e.get("distance"), "fp": e.get("field_position"), "possession": e.get("possession"),
            "text": (e.get("play_text") or "")[:160], "status": e.get("ncaa_status") or "",
            "changes": "; ".join(f"{c.get('field')} {c.get('old')}->{c.get('new')}" for c in ch if isinstance(c, dict))[:120],
            "qc": e.get("qc_issue") or "", "no_board": e.get("no_board") or "",
        })
    nc = (payload.get("qc_summary") or {}).get("ncaa_check") or {}
    return {"game_id": str(game_id), "status": payload.get("status"), "fetched_at": payload.get("fetched_at"),
            "home_name": payload.get("home_name"), "away_name": payload.get("away_name"),
            "check": {k: nc.get(k) for k in ("source", "verified", "fixed", "added", "unverified", "clock_fixes")},
            "quarters": quarter_shares(entries), "withdrawn": payload.get("withdrawn_keys") or [], "rows": rows}
