"""Third backup for TIMEOUTS: Fox Sports' play-by-play has explicit named timeout rows ("Timeout #1 by SMU.",
"Two minute timeout.", "TV Timeout") that neither the crew's feed nor CBS has (Sep 20 2026 finding: Fox's play text
is the same provider feed as CBS, so it is NOT a new observer for plays - but the named timeout rows are a signal CBS
only exposes as remaining-counters). Roger, Oct 24 2026, after SMU vs California's timeouts came out wrong with the
three existing observers disagreeing: "Build it."

Shapes (measured Oct 24-25 2026):
  league/scores                         -> selectionGroupList[].selectionList[] = weeks {id "2026-8-1", date (UTC start)}
  league/scores-segment/<week>?groupId=G -> sectionList[] (one per DATE, id "2026-8-1-20261024") .events[] =
                                           {id "cfb43822", eventTime (UTC), eventStatus (1 pre, 2 live, 3 final),
                                            upperTeam/lowerTeam {name "CAL", longName "California", uri}}
                                           groupId 2 = FBS, 3 = FCS (Fox answers 500 for week 1 FCS)
  event/<id>/data                        -> header.leftTeam/rightTeam {name, longName}; pbp.sections[] {title "1ST QUARTER"
                                           ... "OVERTIME", "2ND OVERTIME", groups[].plays[] {title "2ND AND 9",
                                           subtitle "CAL 33" (spot), playDescription, timeOfPlay "8:38"}}
Fox event ids are Fox's own, not ESPN's; a game is found by UTC date + team names. Fox lists upper/left = away and
lower/right = home, EXCEPT neutral-site games where the two vendors disagree (Prairie View A&M vs Grambling, Sep 26
2026) - so a timeout's side is always resolved by matching the team code in the row to OUR home/away names, never by
position.

Every public function returns a dict and never raises: a backup must not be able to break play delivery.
Offline corpus runs set CAPP_FOX_DIR to a folder of <espn_game_id>_fox.json documents (pulled by the dev tools).
"""
import datetime
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request

BASE = "https://api.foxsports.com/bifrost/v1/cfb/"
# Fox's own public site key (the one its web pages send); overridable without a code change.
API_KEY = os.environ.get("CAPP_FOX_KEY", "jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq")
_TIMEOUT = 8               # the fetch sits in the /plays request path: a cold cache can cost up to four calls (weeks, two segments, event)
_TTL_WEEKS = 3600            # the week list changes once a week
_TTL_SEGMENT = 300           # which games exist on a date: stable; status flips do not matter here
_TTL_EVENT = 20              # the live poller asks every ~10-15 s per open game; one fetch serves two polls
_GROUPS = ("2", "3")         # FBS, then FCS

_cache = {}
_lock = threading.Lock()
_CACHE_MAX = 300
_CACHE_EVICT_SECONDS = 3600


def enabled():
    return os.environ.get("CAPP_FOX", "1") != "0"


def _prune(now):
    """Caller holds _lock."""
    for k in [k for k, v in _cache.items() if now - v[0] > _CACHE_EVICT_SECONDS]:
        del _cache[k]
    if len(_cache) > _CACHE_MAX:
        for k, _ in sorted(_cache.items(), key=lambda kv: kv[1][0])[:len(_cache) - _CACHE_MAX]:
            del _cache[k]


def _call(path, ttl):
    """(status, parsed_json) for a bifrost path, cached. Never raises."""
    url = BASE + path + ("&" if "?" in path else "?") + "apikey=" + API_KEY
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
        _prune(now)
    return status, body


# ── names ──────────────────────────────────────────────────────────────────────────────────────────────────────────
def _norm(s):
    s = str(s or "").lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\bstate\b", "st", s)
    s = re.sub(r"\buniversity\b|\bthe\b|\bcollege\b", " ", s)
    return " ".join(s.split())


def _team_matches(fox_team, espn_name, espn_abbrev):
    """Does a Fox team card name the ESPN team? Fox longName ("California") is the school; ESPN displayName is
    school + nickname ("California Golden Bears"), so the school must open the display name. Fox name ("CAL") is an
    abbreviation and may equal ESPN's. Measured Oct 25 2026: 785 of 785 reachable corpus games matched this way."""
    fl, fn = _norm(fox_team.get("longName") or fox_team.get("long")), _norm(fox_team.get("name"))
    en, ea = _norm(espn_name), _norm(espn_abbrev)
    if fl and (en == fl or en.startswith(fl + " ")):
        return True
    if fn and ea and fn == ea:
        return True
    if fl and ea and fl == ea:                       # Fox longName "SMU" / "UCF" / "BYU" vs ESPN abbreviation
        return True
    return False


# ── finding the game ───────────────────────────────────────────────────────────────────────────────────────────────
def _utc_date(iso):
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(iso or ""))
    return datetime.date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def _week_for(day):
    """The Fox week id whose start date is the latest one on or before the day (plus the one after, for the
    boundary), from league/scores. [] when Fox did not answer."""
    status, doc = _call("league/scores", _TTL_WEEKS)
    weeks = []
    for g in (doc or {}).get("selectionGroupList") or []:
        for s in g.get("selectionList") or []:
            d = _utc_date(s.get("date"))
            if d and s.get("id"):
                weeks.append((d, str(s["id"])))
    weeks.sort()
    if not weeks:
        return []
    before = [w for w in weeks if w[0] <= day]
    out = [before[-1][1]] if before else [weeks[0][1]]
    after = [w for w in weeks if w[0] > day]
    if after and (after[0][0] - day).days <= 1:     # a game listed under next week's first date
        out.append(after[0][1])
    return out


def _events_on(day):
    """Every Fox event (FBS, then FCS) on a UTC date, as the segment lists them."""
    out = []
    for week in _week_for(day):
        for grp in _GROUPS:
            status, seg = _call(f"league/scores-segment/{week}?groupId={grp}", _TTL_SEGMENT)
            for sec in (seg or {}).get("sectionList") or []:
                for ev in sec.get("events") or []:
                    if _utc_date(ev.get("eventTime")) == day:
                        out.append(ev)
    return out


def find_game(game_date, home_name, away_name, home_abbrev="", away_abbrev=""):
    """The Fox event for an ESPN game: {"available", "fox_id", "swapped", "home_team", "away_team", "note"}.
    Both teams named on one event that UTC day -> that event ("swapped" when Fox lists our home team as its away
    team - neutral sites). One team named and the other side of that event names NO other game that day -> that
    event (matched on one team: "NC State" vs Fox's "North Carolina St"). Anything else -> not available."""
    try:
        day = _utc_date(game_date)
        if day is None:
            return {"available": False, "note": "no game date"}
        both, one = [], []
        for ev in _events_on(day):
            fid = re.sub(r"\D", "", str(ev.get("id") or ""))
            up, lo = ev.get("upperTeam") or {}, ev.get("lowerTeam") or {}
            if not fid or not up or not lo:
                continue
            h_lo, a_up = _team_matches(lo, home_name, home_abbrev), _team_matches(up, away_name, away_abbrev)
            h_up, a_lo = _team_matches(up, home_name, home_abbrev), _team_matches(lo, away_name, away_abbrev)
            if h_lo and a_up:
                both.append((fid, False, lo, up))
            elif h_up and a_lo:
                both.append((fid, True, up, lo))
            elif h_lo or a_up:
                one.append((fid, False, lo, up))
            elif h_up or a_lo:
                one.append((fid, True, up, lo))
        pick = both if len(both) == 1 else (one if not both and len(one) == 1 else [])
        if not pick:
            return {"available": False, "note": "no Fox game for these teams on this date" if not both and not one
                    else "more than one Fox game fits these teams on this date"}
        fid, swapped, ht, at = pick[0]
        return {"available": True, "fox_id": fid, "swapped": swapped,
                "home_team": {"name": ht.get("name"), "long": ht.get("longName")},
                "away_team": {"name": at.get("name"), "long": at.get("longName")},
                "note": "" if both else "matched on one team"}
    except Exception as e:
        return {"available": False, "note": f"{type(e).__name__}: {e}"}


def event(fox_id):
    """The Fox event document, or None."""
    status, doc = _call(f"event/{fox_id}/data", _TTL_EVENT)
    return doc if status == 200 and isinstance(doc, dict) else None


# ── timeouts ───────────────────────────────────────────────────────────────────────────────────────────────────────
_QUARTER = {"1ST QUARTER": 1, "2ND QUARTER": 2, "3RD QUARTER": 3, "4TH QUARTER": 4, "OVERTIME": 5, "1ST OVERTIME": 5,
            "2ND OVERTIME": 6, "3RD OVERTIME": 7, "4TH OVERTIME": 8, "5TH OVERTIME": 9, "6TH OVERTIME": 10}
# "Timeout #1 by SMU." on its own, or glued to the end of a play ("... challenged ... Timeout #1 by TENN.")
_TEAM_TO = re.compile(r"timeout\s*#?\s*(\d+)?\s*by\s+([A-Za-z0-9&()'./ -]+?)\.?\s*$", re.I)
_TWO_MIN = re.compile(r"two[\s-]*minute", re.I)
_TV = re.compile(r"\b(tv|media|television|official'?s?)\b.*time\s*out|time\s*out.*\b(tv|media|television|official'?s?)\b", re.I)
_ANY_TO = re.compile(r"time\s*out", re.I)


def _quarter_of(title):
    t = str(title or "").strip().upper()
    if t in _QUARTER:
        return _QUARTER[t]
    m = re.match(r"(\d+)(?:ST|ND|RD|TH)\s+OVERTIME", t)
    if m:
        return 4 + int(m.group(1))
    m = re.match(r"(\d+)(?:ST|ND|RD|TH)\s+QUARTER", t)
    return int(m.group(1)) if m else None


def _secs(clock):
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(clock or ""))
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _sides(cards, home_name, away_name, home_abbrev="", away_abbrev=""):
    """{Fox team code: "home"|"away"} for the two header cards, resolved AGAINST EACH OTHER. "Florida" opens both
    "Florida Gators" and "Florida Atlantic Owls" (Texas / Texas State, Washington / Washington State the same), so a
    card that fits both sides takes the side the other card does not (measured Oct 25 2026: 22 team rows in 3 games
    were charged to nobody before this)."""
    fits = []
    for c in cards:
        code = _norm(c.get("name"))
        if not code:
            continue
        fits.append((code, {s for s, n, a in (("home", home_name, home_abbrev), ("away", away_name, away_abbrev))
                            if _team_matches(c, n, a)}))
    out = {}
    for k, (code, mine) in enumerate(fits):
        others = [o for j, (oc, o) in enumerate(fits) if j != k]
        if len(mine) == 1:
            out[code] = next(iter(mine))
        elif len(mine) == 2 and others and len(others[0]) == 1:
            out[code] = next(iter(mine - others[0]))
    return out


def timeouts(doc, home_name, away_name, home_abbrev="", away_abbrev=""):
    """The named timeout rows of a Fox event document, oldest first:
    [{quarter, clock, secs, charged: "home"|"away"|"officials"|None, kind: "team"|"two-minute"|"tv"|"other",
      number, team: Fox's code, text}]
    A team row's side comes from matching its code ("CAL") to the header's two team cards and then to OUR names -
    never from left/right position (neutral sites). A code that matches neither side is charged None."""
    out = []
    if not isinstance(doc, dict):
        return out
    header = doc.get("header") or {}
    cards = [c for c in (header.get("leftTeam"), header.get("rightTeam")) if isinstance(c, dict)]
    side_of = _sides(cards, home_name, away_name, home_abbrev, away_abbrev)
    for sec in (doc.get("pbp") or {}).get("sections") or []:
        q = _quarter_of(sec.get("title"))
        for grp in sec.get("groups") or []:
            for p in grp.get("plays") or []:
                text = str(p.get("playDescription") or "").strip()
                if not _ANY_TO.search(text):
                    continue
                row = {"quarter": q, "clock": str(p.get("timeOfPlay") or ""), "secs": _secs(p.get("timeOfPlay")),
                       "charged": None, "kind": "other", "number": None, "team": "", "text": text}
                m = _TEAM_TO.search(text)
                if m:
                    row["kind"], row["team"] = "team", m.group(2).strip()
                    row["number"] = int(m.group(1)) if m.group(1) else None
                    row["charged"] = side_of.get(_norm(row["team"]))
                elif _TWO_MIN.search(text):
                    row["kind"], row["charged"] = "two-minute", "officials"
                elif _TV.search(text):
                    row["kind"], row["charged"] = "tv", "officials"
                out.append(row)
    return out


def for_game(game_id, game_date, home_name, away_name, home_abbrev="", away_abbrev=""):
    """Resolve + fetch + parse in one call: {"available", "timeouts", "fox_id", "swapped", "status", "note"}.
    Offline (CAPP_FOX_DIR): reads <dir>/<game_id>_fox.json instead of calling Fox."""
    try:
        if not enabled():
            return {"available": False, "timeouts": [], "note": "disabled"}
        d = os.environ.get("CAPP_FOX_DIR")
        if d:
            path = os.path.join(d, f"{game_id}_fox.json")
            if not os.path.exists(path):
                return {"available": False, "timeouts": [], "note": "no saved document"}
            doc = json.load(open(path, encoding="utf-8"))
            cap = doc.get("_capp") or {}
            return {"available": True, "fox_id": str(cap.get("fox_id") or ""), "swapped": bool(cap.get("swapped")),
                    "status": (doc.get("header") or {}).get("eventStatus"),
                    "timeouts": timeouts(doc, home_name, away_name, home_abbrev, away_abbrev), "note": "saved document"}
        found = find_game(game_date, home_name, away_name, home_abbrev, away_abbrev)
        if not found.get("available"):
            return {"available": False, "timeouts": [], "note": found.get("note", "")}
        doc = event(found["fox_id"])
        if doc is None:
            return {"available": False, "timeouts": [], "fox_id": found["fox_id"], "note": "Fox did not answer for the event"}
        return {"available": True, "fox_id": found["fox_id"], "swapped": found["swapped"],
                "status": (doc.get("header") or {}).get("eventStatus"),
                "timeouts": timeouts(doc, home_name, away_name, home_abbrev, away_abbrev), "note": found.get("note", "")}
    except Exception as e:
        return {"available": False, "timeouts": [], "note": f"{type(e).__name__}: {e}"}
