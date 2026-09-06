"""NCAA.com play-by-play — the source that actually LABELS a timeout.

Built Sep 6 2026, the morning after a Saturday where two problems cost real work:

  1. **Timeouts.** ESPN publishes every stoppage as the same thing: type 21,
     "Timeout", `teamParticipants[].timeout = true`, on both a real team timeout
     and a TV/officials/injury one. CFBD is derived from the same rows (its
     `offenseTimeouts`/`defenseTimeouts` run to -1 and -2, which is arithmetic on
     bad input, not a second opinion). So SBGEN charged all of them and teams sat
     at zero timeouts by the second quarter.

     NCAA.com writes the answer in plain text:

         "Timeout Hampton, clock 12:40."     <- charged to Hampton
         "Timeout Other, clock 09:44."       <- officials / TV / injury / 2-min

     Measured on Hampton @ Maryland (NCAA game 6603983): 14 timeout rows, of
     which **3** are charged to a team and 11 are "Other". Our saved second half
     had six timeout rows and exactly ONE of them was real.

  2. **Stuck clocks.** When a stat crew stops advancing the clock, ESPN stamps a
     whole run of plays with one time. NCAA is a separate transcription, so its
     run is usually intact.

⚠ WHAT THIS MODULE IS NOT. It is a **verification and repair source, never a
replacement**. ESPN stays primary. Nothing here writes to a game; the endpoints
return proposals and the coach applies them. Reasons, all learned the hard way:
  - `driveText` is sometimes incomplete or malformed ("1 and 10 at 35" with no
    side of the field).
  - NCAA's play list is longer than ESPN's — it carries "UMD ball on UMD35.",
    drive-start markers and coin-toss lines that are not plays. 219 NCAA rows
    against our 182 for the same game. A positional walk WILL drift; that exact
    mistake paired a Q1 4:36 against a 2:58 during the Bleacher Report attempt.
  - NCAA has changed or disabled its underlying endpoints before.

⚠ IDS ARE NOT ESPN IDS. NCAA has its own contest ids (6603983), so every lookup
starts by resolving teams + date against the scoreboard. CFBD shares ESPN's ids;
NCAA does not.

DEPLOYMENT. The public host (https://ncaa-api.henrygd.me) is a demo and caches
for ~60s; fine for manual post-quarter repair, not something to build on. Set
NCAA_API_URL to a self-hosted private service (docker.io/henrygd/ncaa-api, port
3000) and NCAA_API_KEY to the value of its NCAA_HEADER_KEY, sent as x-ncaa-key.
"""

import json
import os
import re
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

NCAA_BASE = (os.environ.get("NCAA_API_URL") or "https://ncaa-api.henrygd.me").rstrip("/")
_TIMEOUT = 12

# The upstream wrapper caches for ~60s anyway; this keeps us from hammering it
# when a coach clicks through several quarters in a row. Repair is manual and
# post-hoc, so a stale minute costs nothing.
_CACHE_SECONDS = 45
_SCOREBOARD_CACHE_SECONDS = 300     # scoreboards for a past week never change
_cache: dict = {}
_lock = threading.Lock()


def _api_key() -> str:
    return (os.environ.get("NCAA_API_KEY") or "").strip()


def using_public_host() -> bool:
    """True when we are on the shared demo host rather than our own service."""
    return "henrygd.me" in NCAA_BASE


def _call(path: str, ttl: int = _CACHE_SECONDS):
    """(status, parsed_json), cached. Never raises — a second source must never
    be able to take down the endpoint that uses it."""
    now = time.time()
    with _lock:
        hit = _cache.get(path)
        if hit and now - hit[0] < ttl:
            return hit[1], hit[2]

    headers = {"Accept": "application/json", "User-Agent": "CAPP/1.0"}
    key = _api_key()
    if key:
        headers["x-ncaa-key"] = key
    req = urllib.request.Request(f"{NCAA_BASE}{path}", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            status, body = resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            status, body = e.code, json.loads(e.read() or b"null")
        except Exception:
            status, body = e.code, None
    except Exception:
        status, body = 0, None

    with _lock:
        _cache[path] = (now, status, body)
    return status, body


# ── team-name matching ────────────────────────────────────────────────────────
# NCAA writes "Oregon St.", "Miami (OH)", "Army West Point", "App State" where
# CAPP/ESPN write "Oregon State", "Miami (OH)", "Army", "Appalachian State".
# Fold hard, then allow a small alias table for the ones folding cannot reach.
_ALIASES = {
    "army west point": "army",
    "app state": "appalachian state",
    "ole miss": "mississippi",
    "southern miss": "southern mississippi",
    "hawaii": "hawai i",
    "uconn": "connecticut",
    "umass": "massachusetts",
    "utsa": "texas san antonio",
    "utep": "texas el paso",
    "ucf": "central florida",
    "fiu": "florida international",
    "fau": "florida atlantic",
    "smu": "southern methodist",
    "tcu": "texas christian",
    "lsu": "louisiana state",
    "byu": "brigham young",
    "usc": "southern california",
    "unlv": "nevada las vegas",
    "niu": "northern illinois",
    "ndsu": "north dakota state",
    "sdsu": "south dakota state",
}


def _fold(s) -> str:
    """Lowercase, strip accents and punctuation, expand 'St.' to 'State'.

    The accent fold is not decoration: San José State comes back from one source
    with the accent and the other without, and an exact compare drops the game.
    """
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = s.replace("&", " and ")
    s = re.sub(r"\bst\.?\b", "state", s)          # Oregon St. -> Oregon State
    s = re.sub(r"[^a-z0-9() ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return _ALIASES.get(s, s)


def _name_forms(team: dict) -> list:
    """Every string NCAA offers for a team, folded. seo names ('miami-fl') are
    included because they sometimes match a CAPP slug when the display name
    does not."""
    names = team.get("names", team) or {}
    out = []
    for k in ("short", "full", "seo", "char6", "nameShort", "nameFull", "seoname", "name6Char"):
        v = names.get(k) or team.get(k)
        if v:
            out.append(_fold(str(v).replace("-", " ")))
    return [x for x in out if x]


def _name_score(want: str, forms: list) -> int:
    """0 = no, 1 = weak (containment), 2 = strong (exact fold)."""
    w = _fold(want)
    if not w:
        return 0
    if w in forms:
        return 2
    for f in forms:
        if not f:
            continue
        if w == f:
            return 2
        # "miami" against "miami (fl)" or "north carolina" vs "north carolina st"
        if w in f or f in w:
            return 1
    return 0


# ── public API ────────────────────────────────────────────────────────────────
def scoreboard(year, week, division="fbs", conf="all-conf") -> dict:
    """One week of the NCAA scoreboard. Weeks are zero-padded ('01')."""
    wk = str(week).zfill(2)
    path = f"/scoreboard/football/{division}/{year}/{wk}/{conf}"
    status, body = _call(path, ttl=_SCOREBOARD_CACHE_SECONDS)
    if status != 200 or not isinstance(body, dict):
        return {"available": False, "games": [], "note": f"scoreboard HTTP {status}"}
    games = []
    for item in body.get("games", []):
        g = item.get("game", item)
        games.append({
            "ncaa_game_id": str(g.get("gameID") or ""),
            "home": (g.get("home") or {}).get("names", {}).get("short", ""),
            "away": (g.get("away") or {}).get("names", {}).get("short", ""),
            "home_score": (g.get("home") or {}).get("score", ""),
            "away_score": (g.get("away") or {}).get("score", ""),
            "state": g.get("gameState", ""),
            "period": g.get("currentPeriod", ""),
            "clock": g.get("contestClock", ""),
            "start_date": g.get("startDate", ""),
            "start_time": g.get("startTime", ""),
            "_home_raw": g.get("home") or {},
            "_away_raw": g.get("away") or {},
        })
    return {"available": True, "games": games}


def resolve_game(year, home, away, date=None, weeks=None, division="fbs") -> dict:
    """Find the NCAA contest id for a game CAPP knows by team names.

    `date` is MM/DD/YYYY as NCAA writes it. Without it the search still works but
    a rematch later in the season could tie, so the caller should pass it.
    `weeks` narrows the scan; by default weeks 01-16 are tried in order and the
    scan stops at the first week that contains a confident match.

    Returns {"available", "ncaa_game_id", "confidence", "candidates", ...}.
    confidence: "exact" (both names folded equal), "likely" (one exact + one
    containment), or "none". The caller must treat anything below "likely" as
    unresolved and fall back to manual - never guess a game.
    """
    weeks = weeks or [f"{w:02d}" for w in range(1, 17)]
    best = None
    candidates = []
    for wk in weeks:
        sb = scoreboard(year, wk, division=division)
        if not sb.get("available"):
            continue
        for g in sb["games"]:
            if date and g.get("start_date") and g["start_date"] != date:
                continue
            hs = _name_score(home, _name_forms(g["_home_raw"]))
            aws = _name_score(away, _name_forms(g["_away_raw"]))
            if not (hs and aws):
                continue
            total = hs + aws
            row = {"ncaa_game_id": g["ncaa_game_id"], "home": g["home"], "away": g["away"],
                   "start_date": g["start_date"], "state": g["state"], "week": wk,
                   "score": total}
            candidates.append(row)
            if best is None or total > best["score"]:
                best = row
        if best and best["score"] == 4:
            break            # both names exact - no later week can beat it

    if not best and date:
        # The date the client has is ESPN's, in UTC - a 10pm ET kickoff is the
        # NEXT day there. Rather than guess a timezone, drop the constraint and
        # match on names alone; the ± confidence rules still apply.
        loose = resolve_game(year, home, away, date=None, weeks=weeks, division=division)
        if loose.get("available"):
            loose["note"] = "matched without the date filter"
            return loose
    if not best:
        return {"available": False, "ncaa_game_id": "", "confidence": "none",
                "candidates": [], "note": "no NCAA game matched those teams"}
    confidence = "exact" if best["score"] == 4 else ("likely" if best["score"] == 3 else "weak")
    return {
        "available": confidence in ("exact", "likely"),
        "ncaa_game_id": best["ncaa_game_id"],
        "confidence": confidence,
        "matched_home": best["home"],
        "matched_away": best["away"],
        "start_date": best["start_date"],
        "week": best["week"],
        "candidates": sorted(candidates, key=lambda r: -r["score"])[:5],
    }


def play_by_play(ncaa_game_id) -> dict:
    """Flatten NCAA's periods[].playbyplayStats[].plays[] into one ordered list.

    ⚠ THE NESTING IS TWO DEEP. `playbyplayStats` entries are drive/possession
    groups carrying a teamId, and the actual plays hang off `plays` inside them.
    Reading `periods[].plays` gives an empty list for a perfectly healthy game.
    """
    status, body = _call(f"/game/{ncaa_game_id}/play-by-play")
    if status != 200 or not isinstance(body, dict):
        return {"available": False, "plays": [], "teams": {},
                "note": f"play-by-play HTTP {status}"}

    teams = {}
    for t in body.get("teams", []):
        side = "home" if t.get("isHome") else "away"
        teams[side] = {
            "team_id": str(t.get("teamId") or ""),
            "short": t.get("nameShort", ""),
            "full": t.get("nameFull", ""),
            "abbrev": t.get("name6Char", ""),
            "seo": t.get("seoname", ""),
        }

    plays, seq = [], 0
    for per in body.get("periods", []):
        q = per.get("periodNumber")
        for grp in per.get("playbyplayStats", []) or []:
            gclock = grp.get("clock") or ""
            tid = str(grp.get("teamId") or "")
            for pl in grp.get("plays", []) or []:
                seq += 1
                plays.append({
                    "seq": seq,
                    "quarter": q,
                    "clock": _norm_clock(pl.get("clock") or gclock),
                    "team_id": tid,
                    "drive_text": pl.get("driveText") or "",
                    "home_score": pl.get("homeScore"),
                    "away_score": pl.get("visitorScore"),
                    "text": pl.get("playText") or "",
                })
    return {
        "available": True,
        "status": body.get("status", ""),
        "period": body.get("period"),
        "teams": teams,
        "plays": plays,
        "note": "public demo host" if using_public_host() else "",
    }


def _norm_clock(c) -> str:
    """'09:44' -> '9:44'. CAPP stores M:SS, NCAA pads to MM:SS."""
    c = str(c or "").strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", c)
    return f"{int(m.group(1))}:{m.group(2)}" if m else c


# Timeout lines are the whole point, so parse them narrowly rather than by
# keyword: "Timeout <who>, clock <mm:ss>." Anything that does not fit the shape
# is reported as unclassified instead of being guessed at.
_TIMEOUT_RE = re.compile(r"^\s*timeout\s+(.+?)\s*(?:,\s*clock\s*([\d:]+))?\s*\.?\s*$", re.I)


def timeouts(ncaa_game_id) -> dict:
    """Every timeout in the game, each said to be charged to a team or not.

    charged: "home" | "away" | None (None == officials/TV/injury/two-minute,
    i.e. NCAA wrote "Timeout Other"). `official` is the same fact stated the way
    the client asks the question.
    """
    pbp = play_by_play(ncaa_game_id)
    if not pbp.get("available"):
        return {"available": False, "timeouts": [], "note": pbp.get("note", "")}

    home = pbp["teams"].get("home", {})
    away = pbp["teams"].get("away", {})
    home_forms = [_fold(v) for v in (home.get("short"), home.get("full"),
                                     home.get("abbrev"), (home.get("seo") or "").replace("-", " ")) if v]
    away_forms = [_fold(v) for v in (away.get("short"), away.get("full"),
                                     away.get("abbrev"), (away.get("seo") or "").replace("-", " ")) if v]

    out = []
    for p in pbp["plays"]:
        text = p["text"].strip()
        if not text.lower().startswith("timeout"):
            continue
        m = _TIMEOUT_RE.match(text)
        who = (m.group(1) if m else "").strip()
        clock = _norm_clock(m.group(2) if (m and m.group(2)) else p["clock"])
        folded = _fold(who)

        charged, team_name = None, ""
        if folded in ("other", "", "official", "officials", "media", "tv", "injury"):
            charged = None
        elif _name_score(who, home_forms):
            charged, team_name = "home", home.get("short", "")
        elif _name_score(who, away_forms):
            charged, team_name = "away", away.get("short", "")
        else:
            # A team name we could not tie to either side. Say so; do not guess.
            charged, team_name = "unknown", who

        out.append({
            "quarter": p["quarter"],
            "clock": clock,
            "charged": charged,
            "team": team_name,
            "official": charged is None,
            "text": text,
            "drive_text": p["drive_text"],
            "home_score": p["home_score"],
            "away_score": p["away_score"],
            "seq": p["seq"],
        })

    counts = {"home": 0, "away": 0, "official": 0, "unknown": 0}
    for t in out:
        counts["official" if t["charged"] is None else t["charged"]] += 1
    return {
        "available": True,
        "teams": pbp["teams"],
        "timeouts": out,
        "counts": counts,
        "note": pbp.get("note", ""),
    }
