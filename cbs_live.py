"""CBS play-by-play for the live check (Roger, Sep 14 2026: "ESPN = Primary CBS = Primary Backup for Comparison NCAA =
Secondary Backup for ESPN Bad Plays and CBS not Posting Play By Play").

Finds the CBS game for an ESPN game, fetches CBS's play-by-play and turns it into the same items SBENTRY's Resolve reads
(the Backup Data Builder's "capp-backup-data" shape - CBS words plays exactly like Bleacher Report). Never raises: a
second source must never break play delivery.

Finding the game. ESPN knows a game by its own ids; CBS by its own. Two files next to this one:
    cbs_games.csv          Roger's CBS season list (Week, Date, Away, Home, CBSGameID, Slug) - CBS team codes
    cbs_team_codes.json    CBS team code -> ESPN team id, learned from finished games
                           (_dev_tools/game_replay_test/build_cbs_team_codes.py)
⚠ MEASURED Sep 14 2026 on Sep 12's 79 games: the CBS code equals ESPN's abbreviation for only 62 of 158 teams
("CUSE" = ESPN "SYR", "TXTECH" = "TTU"), so the codes are looked up by ESPN team id, never compared as text. CBS lists
a game under its US date; ESPN stamps UTC (Air Force's Sep 12 night game is "2026-09-13T02:00Z").
⚠ CBS has NO play-by-play for some games (10 of 79 on Sep 12, e.g. Middle Tennessee @ Marshall answers "No scoring
plays data for that game.") - available=False, and the NCAA check runs instead.
"""
import csv
import datetime
import json
import os
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
PLAYS_URL = "https://api.cbssports.com/napi/resource/game/scoring/plays/{}"
BOARD_URL = "https://api.cbssports.com/napi/resource/game/scoring/scoreboard/{}"
_TIMEOUT = 12
_CACHE_SECONDS = 20          # the live poller asks every ~10 s per open game; one CBS fetch serves two polls

_cache = {}
_lock = threading.Lock()
_tables = {"games": None, "tid_codes": None}


def _call(url, ttl=_CACHE_SECONDS):
    """(status, parsed_json), cached. Never raises."""
    now = time.time()
    with _lock:
        hit = _cache.get(url)
        if hit and now - hit[0] < ttl:
            return hit[1], hit[2]
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as r:
            status, body = r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        status, body = e.code, None
    except Exception:
        status, body = 0, None
    with _lock:
        _cache[url] = (now, status, body)
    return status, body


def _load_tables():
    if _tables["games"] is None:
        by_date = {}
        try:
            for r in csv.DictReader(open(os.path.join(HERE, "cbs_games.csv"), encoding="utf-8")):
                by_date.setdefault(str(r.get("Date") or ""), []).append(r)
        except Exception as e:
            print(f"WARNING: cbs_games.csv not loaded: {type(e).__name__}: {e}", flush=True)
        _tables["games"] = by_date
    if _tables["tid_codes"] is None:
        tid_codes = {}
        try:
            codes = json.load(open(os.path.join(HERE, "cbs_team_codes.json"), encoding="utf-8")).get("codes") or {}
            for code, v in codes.items():
                tid_codes.setdefault(str(v.get("espn_team_id") or ""), set()).add(code)
        except Exception as e:
            print(f"WARNING: cbs_team_codes.json not loaded: {type(e).__name__}: {e}", flush=True)
        _tables["tid_codes"] = tid_codes
    return _tables["games"], _tables["tid_codes"]


def _us_dates(game_date):
    """ESPN's UTC stamp -> the dates CBS may list the game under: the UTC date, then the day before (a night game)."""
    try:
        d = datetime.datetime.strptime(str(game_date or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        return []
    return [d.strftime("%Y%m%d"), (d - datetime.timedelta(days=1)).strftime("%Y%m%d")]


def find_game(game_date, home_team_id, away_team_id):
    """The CBS game for an ESPN game: {"available", "cbs_game_id", "home_code", "away_code", "swapped", "note"}.

    Both teams' codes known and on one game that date -> that game. One team known and the other team's code is not in
    the table at all (a team CBS lists that has not played a matched game yet) -> that game too: a team plays one game
    a date. Anything else -> not available (never a guess)."""
    if not enabled():
        return {"available": False, "note": "CBS turned off (CAPP_CBS=0)"}
    games, tid_codes = _load_tables()
    home, away = tid_codes.get(str(home_team_id), set()), tid_codes.get(str(away_team_id), set())
    known = set().union(*tid_codes.values()) if tid_codes else set()
    if not home and not away:
        return {"available": False, "note": "neither team is in the CBS team-code table"}
    for day in _us_dates(game_date):
        both, one = [], []
        for r in games.get(day, []):
            h, a = r.get("Home", ""), r.get("Away", "")
            if h in home and a in away:
                both.append((r, False))
            elif h in away and a in home:
                both.append((r, True))
            elif (h in home and a not in known) or (a in away and h not in known):
                one.append((r, False))
            elif (h in away and a not in known) or (a in home and h not in known):
                one.append((r, True))
        pick = both if both else one
        if len(pick) == 1:
            r, swapped = pick[0]
            return {"available": True, "cbs_game_id": str(r["CBSGameID"]), "home_code": r["Home"], "away_code": r["Away"],
                    "swapped": swapped, "date": day, "note": "" if both else "matched on one team"}
        if len(pick) > 1:
            return {"available": False, "note": f"{len(pick)} CBS games fit on {day} - not picking one"}
    return {"available": False, "note": "no CBS game for these teams on this date"}


def enabled():
    """CAPP_CBS=0 turns CBS off - only the _dev_tools tests of the NCAA path set it; the server never does."""
    return os.environ.get("CAPP_CBS", "1") != "0"


def play_by_play(cbs_game_id):
    """{"available", "plays", "note"}. CBS answers a game it has no play-by-play for with HTTP 200 and a warning.
    CAPP_CBS_DIR (tests only): read saved <dir>/<cbs id>_plays.json and never touch the network."""
    if not enabled():
        return {"available": False, "plays": [], "note": "CBS turned off (CAPP_CBS=0)"}
    folder = os.environ.get("CAPP_CBS_DIR") or ""
    if folder:
        try:
            status, body = 200, json.load(open(os.path.join(folder, f"{cbs_game_id}_plays.json"), encoding="utf-8"))
        except Exception:
            status, body = 0, None
    else:
        status, body = _call(PLAYS_URL.format(cbs_game_id))
    plays = body.get("plays") if isinstance(body, dict) else None
    if status == 200 and plays:
        return {"available": True, "plays": plays, "note": ""}
    note = ""
    if isinstance(body, dict) and body.get("warnings"):
        note = str((body["warnings"][0] or {}).get("message") or "")
    return {"available": False, "plays": [], "note": note or f"CBS HTTP {status}"}


def scoreboard(cbs_game_id):
    """{"available", "status", "quarter", "home_team_id", "away_team_id"} - CBS's own game state (FINAL / in progress)
    and CBS's ids for its home and away team (every play's team_in_possession is one of them - cbs_backup.py).
    CAPP_CBS_DIR (tests only): read saved <dir>/<cbs id>_scoreboard.json and never touch the network."""
    folder = os.environ.get("CAPP_CBS_DIR") or ""
    if folder:
        try:
            status, body = 200, json.load(open(os.path.join(folder, f"{cbs_game_id}_scoreboard.json"), encoding="utf-8"))
        except Exception:
            status, body = 0, None
    else:
        status, body = _call(BOARD_URL.format(cbs_game_id))
    sb = body.get("scoreboard") if isinstance(body, dict) else None
    gs = sb.get("game_status") if isinstance(sb, dict) else None
    if status != 200 or not isinstance(gs, dict):
        return {"available": False, "status": "", "quarter": None, "home_team_id": "", "away_team_id": ""}
    return {"available": True, "status": str(gs.get("status") or ""), "quarter": gs.get("quarter"),
            "home_team_id": str((sb.get("hometeam") or {}).get("id") or ""),
            "away_team_id": str((sb.get("awayteam") or {}).get("id") or "")}


def _int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _norm_clock(c):
    parts = str(c or "").strip().split(":")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and len(parts[1]) == 2:
        return "%d:%s" % (int(parts[0]), parts[1])
    return ""


def to_items(plays, home_code, away_code, swapped=False):
    """CBS plays -> Resolve items (kind play / kickoff / extra_point / two_point; CBS writes no timeout plays).
    swapped: CBS lists the home team as the away team (neutral site) - the spot side follows ESPN's home/away."""
    side_of = {str(home_code).upper(): "away" if swapped else "home", str(away_code).upper(): "home" if swapped else "away"}
    items, skipped = [], {}
    for p in plays or []:
        subs = ((p.get("subplays") or {}).get("subplay")) or []
        if isinstance(subs, dict):
            subs = [subs]
        types = {s.get("type") for s in subs if isinstance(s, dict)}
        text = str(p.get("description") or "").strip()
        low = text.lower()
        clock = _norm_clock(p.get("time_remaining"))
        m, s = (clock.split(":") if clock else (None, None))
        base = {"quarter": _int(p.get("quarter")) or 0, "clock": clock,
                "clock_secs": int(m) * 60 + int(s) if clock else None, "text": text,
                "cbs_id": str(p.get("id") or ""), "real_clock": str(p.get("real_clock") or ""),
                "team_id": str(p.get("team_in_possession") or "")}
        if low.startswith("two-point conversion"):
            kind = "two_point"
        elif "Kickoff" in types or " kicks " in f" {low} ":
            kind = "kickoff"
        elif types & {"PointAfterTouchdown", "MissedPointAfterTouchdown"} or "extra point" in low:
            kind = "extra_point"
        else:
            down = _int(p.get("down"))
            if not down or not 1 <= down <= 4:
                key = f"down {p.get('down')!r}"
                skipped[key] = skipped.get(key, 0) + 1
                continue
            kind = "play"
            side = str(p.get("side") or "").upper()
            yard = _int(p.get("yardline"))
            # "1st & Goal at the 6": CBS writes the word "Goal" - the distance is the yards to the goal line. MEASURED
            # Sep 14 2026: 494 "Goal" plays on 69 games; without this UCF @ Pitt's Q4 goal-line run (ESPN 1&6) did not
            # match ESPN's copy and was added a second time.
            goal = str(p.get("distance") or "").strip().lower() == "goal"
            base.update(down=down, distance=yard if goal else _int(p.get("distance")), goal_to_go=goal, spot_code=side,
                        spot_yard=yard, spot_side=side_of.get(side))
        items.append(dict(base, kind=kind))
    for seq, it in enumerate(items, 1):
        it["seq"] = seq
    return {"format": "capp-backup-data", "version": 1, "source": "cbs", "items": items, "has_timeouts": False,
            "skipped": skipped, "unparsed": [], "errors": []}
