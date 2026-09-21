"""
CAPP Data Server - ESPN Fetcher with Full Play Mapping Pipeline
Ports the complete mapping logic from CAPP's espn_live.py so the server
returns fully CAPP-ready play entries to clients.
"""

import re
from collections import Counter
import requests
import threading
import time
import unicodedata


def _strip_accents(s: str) -> str:
    """Normalize accented characters to ASCII equivalents (e.g. José → Jose)."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

# ============================================================
# ESPN API URLs
# ============================================================
CFB_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
NFL_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
CFB_SUMMARY_URL    = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary"
NFL_SUMMARY_URL    = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"

REQUEST_TIMEOUT = 15
POLL_INTERVAL   = 30     # legacy full-sweep interval — no longer used by _poll_loop

# ── Demand-driven polling (Jul 30 2026) ──────────────────────────────────────
# ESPN is called ONLY for games a client actually has open, and only as often as
# that game's state warrants. With nobody watching, the poller makes ZERO
# requests — it does not sweep the scoreboard and does not care what day it is.
# Rationale + full design: see _poll_loop() and ROADMAP "Demand-driven ESPN polling".
ACTIVE_GAME_TTL       = 180   # drop a game this long after the last client heartbeat
POLL_LIVE_SECONDS     = 10    # status "in"  — full speed
POLL_PRE_SECONDS      = 60    # status "pre" — watching for kickoff
POLL_PRE_SLOW_SECONDS = 300   # a "pre" game that clearly isn't imminent
PRE_FAST_WINDOW       = 900   # poll "pre" at 60s for this long, then back off to slow
POST_GRACE_SECONDS    = 300   # hold a final game briefly for stat corrections, then stop
POLLER_TICK           = 2     # loop wake-up; real work is scheduled per game
LIVE_LIST_TTL         = 30    # /games scoreboard: fetched on demand, cached this long

_session = requests.Session()

# ============================================================
# Team Name Data (ported from espn_live.py)
# ============================================================
ESPN_NAME_OVERRIDES = {
    "Hawai'i Rainbow Warriors": "Hawai'i",
    "Hawaii Rainbow Warriors": "Hawai'i",
    "Hawai'i": "Hawai'i",
    "Hawaii": "Hawai'i",
    "Appalachian State Mountaineers": "App State",
    "Appalachian State": "App State",
    "App State": "App State",
    "Brigham Young Cougars": "BYU",
    "BYU Cougars": "BYU",
    "Charlotte 49ers": "Charlotte",
    "UNC Charlotte 49ers": "Charlotte",
    "UNC Charlotte": "Charlotte",
    "FIU Panthers": "FIU",
    "Louisiana Ragin' Cajuns": "Louisiana",
    "Louisiana-Lafayette Ragin' Cajuns": "Louisiana",
    "Louisiana Ragin Cajuns": "Louisiana",
    "UL Monroe Warhawks": "Louisiana-Monroe",
    "Louisiana-Monroe Warhawks": "Louisiana-Monroe",
    "ULM Warhawks": "Louisiana-Monroe",
    "LSU Tigers": "LSU",
    "Miami Hurricanes": "Miami",
    "Miami (FL) Hurricanes": "Miami",
    "Miami RedHawks": "Miami (OH)",
    "Miami (OH) RedHawks": "Miami (OH)",
    "NC State Wolfpack": "NC State",
    "North Carolina State Wolfpack": "NC State",
    "Ole Miss Rebels": "Ole Miss",
    "Mississippi Rebels": "Ole Miss",
    "Mississippi State Bulldogs": "Mississippi State",
    "SMU Mustangs": "SMU",
    "Southern Methodist Mustangs": "SMU",
    "Southern Miss Golden Eagles": "Southern Miss",
    "Southern Mississippi Golden Eagles": "Southern Miss",
    "TCU Horned Frogs": "TCU",
    "Texas Christian Horned Frogs": "TCU",
    "Texas Christian": "TCU",
    "UAB Blazers": "UAB",
    "UCF Knights": "UCF",
    "UCLA Bruins": "UCLA",
    "UConn Huskies": "UConn",
    "Connecticut Huskies": "UConn",
    "UMass Minutemen": "UMass",
    "Massachusetts Minutemen": "UMass",
    "UNLV Rebels": "UNLV",
    "USC Trojans": "USC",
    "UTEP Miners": "UTEP",
    "UTSA Roadrunners": "UTSA",
    "UT Rio Grande Valley Vaqueros": "UTRGV",
    "UT Rio Grande Valley": "UTRGV",
    "UTRGV Vaqueros": "UTRGV",
    "South Florida Bulls": "South Florida",
    "USF Bulls": "South Florida",
    "Sam Houston Bearkats": "Sam Houston",
    "Sam Houston State Bearkats": "Sam Houston",
    "LIU Sharks": "LIU",
    "VMI Keydets": "VMI",
    "Bethune-Cookman Wildcats": "Bethune-Cookman",
    "Arkansas-Pine Bluff Golden Lions": "Arkansas-Pine Bluff",
    "North Carolina A&T Aggies": "North Carolina A&T",
    "North Carolina Central Eagles": "North Carolina Central",
    "Prairie View A&M Panthers": "Prairie View A&M",
    "Texas A&M-Commerce Lions": "Texas A&M-Commerce",
    "Stephen F. Austin Lumberjacks": "Stephen F. Austin",
    "Houston Christian Huskies": "Houston Christian",
    "Southeast Missouri State Redhawks": "Southeast Missouri State",
    "UT Martin Skyhawks": "UT Martin",
    "St. Thomas (MN)": "St. Thomas (MN)",
    "St. Thomas-Minnesota Tommies": "St. Thomas (MN)",
    "St. Thomas-Minnesota": "St. Thomas (MN)",
    "The Citadel Bulldogs": "Citadel",
    "The Citadel": "Citadel",
    "Pennsylvania Quakers": "Penn",
    "Pennsylvania": "Penn",
    "Indiana State Sycamores": "Indiana State",
    "Saint Francis (PA)": "Saint Francis (PA)",
    "Saint Francis Red Flash": "Saint Francis (PA)",
    "Delaware Blue Hens": "Delaware",
    "Long Island University Sharks": "LIU",
    "Grambling Tigers": "Grambling State",
    "SE Louisiana Lions": "Southeastern Louisiana",
    "UAlbany Great Danes": "Albany",
    "East Texas A&M Lions": "Texas A&M-Commerce",
    "Delaware Blue Hens": "Delaware",
    "West Georgia Wolves": "West Georgia",
    "New Haven Chargers": "New Haven",
    "San Jos\u00e9 State Spartans": "San Jose State",
    "San Jose State Spartans": "San Jose State",
}

CAPP_TEAM_NAMES = {
    "Air Force", "Akron", "Alabama", "App State", "Arizona",
    "Arizona State", "Arkansas", "Arkansas State", "Army", "Auburn",
    "Ball State", "Baylor", "Boise State", "Boston College", "Bowling Green",
    "Buffalo", "BYU", "California", "Central Michigan", "Charlotte",
    "Cincinnati", "Clemson", "Coastal Carolina", "Colorado", "Colorado State",
    "Duke", "East Carolina", "Eastern Michigan", "FIU", "Florida",
    "Florida Atlantic", "Florida State", "Fresno State", "Georgia", "Georgia Southern",
    "Georgia State", "Georgia Tech", "Hawai'i", "Houston", "Illinois",
    "Indiana", "Iowa", "Iowa State", "Jacksonville State", "James Madison",
    "Kansas", "Kansas State", "Kent State", "Kentucky", "Kennesaw State",
    "Liberty", "Louisiana", "Louisiana-Monroe", "Louisiana Tech", "Louisville",
    "LSU", "Marshall", "Maryland", "Memphis", "Miami", "Miami (OH)",
    "Michigan", "Michigan State", "Middle Tennessee", "Minnesota", "Mississippi State",
    "Missouri", "Navy", "NC State", "Nebraska", "Nevada", "New Mexico",
    "New Mexico State", "North Carolina", "North Texas", "Northern Illinois",
    "Northwestern", "Notre Dame", "Ohio", "Ohio State", "Oklahoma",
    "Oklahoma State", "Old Dominion", "Ole Miss", "Oregon", "Oregon State",
    "Penn State", "Pittsburgh", "Purdue", "Rice", "Rutgers", "Sam Houston",
    "San Diego State", "San Jose State", "SMU", "South Alabama", "South Carolina",
    "South Florida", "Southern Miss", "Stanford", "Syracuse", "TCU",
    "Temple", "Tennessee", "Texas", "Texas A&M", "Texas State",
    "Texas Tech", "Toledo", "Troy", "Tulane", "Tulsa", "UAB",
    "UCF", "UCLA", "UConn", "UMass", "UNLV", "USC", "Utah",
    "Utah State", "UTEP", "UTSA", "Vanderbilt", "Virginia",
    "Virginia Tech", "Wake Forest", "Washington", "Washington State",
    "West Virginia", "Western Kentucky", "Western Michigan", "Wisconsin",
    "Wyoming",
    "Abilene Christian", "Alabama A&M", "Alabama State", "Albany",
    "Alcorn State", "Arkansas-Pine Bluff", "Austin Peay", "Bethune-Cookman",
    "Brown", "Bryant", "Bucknell", "Butler", "Cal Poly", "Campbell",
    "Central Arkansas", "Central Connecticut", "Charleston Southern", "Chattanooga",
    "Citadel", "Colgate", "Columbia", "Cornell", "Dartmouth", "Davidson",
    "Dayton", "Delaware", "Delaware State", "Drake", "Duquesne", "East Tennessee State",
    "Eastern Illinois", "Eastern Kentucky", "Eastern Washington", "Elon",
    "Florida A&M", "Fordham", "Furman", "Gardner-Webb", "Georgetown",
    "Grambling State", "Hampton", "Harvard", "Holy Cross", "Houston Baptist",
    "Houston Christian", "Howard", "Idaho", "Idaho State", "Illinois State",
    "Incarnate Word", "Indiana State", "Jackson State", "LIU", "Lafayette",
    "Lamar", "Lehigh", "Lindenwood", "Maine", "Marist", "McNeese",
    "Mercer", "Mercyhurst", "Merrimack", "Mississippi Valley State",
    "Missouri State", "Monmouth", "Montana", "Montana State", "Morehead State",
    "Morgan State", "Murray State", "New Hampshire", "New Haven", "Nicholls", "Norfolk State",
    "North Alabama", "North Carolina A&T", "North Carolina Central", "North Dakota",
    "North Dakota State", "Northern Arizona", "Northern Colorado", "Northern Iowa",
    "Northwestern State", "Penn", "Portland State", "Prairie View A&M",
    "Presbyterian", "Princeton", "Rhode Island", "Richmond", "Robert Morris",
    "Sacramento State", "Sacred Heart", "Saint Francis (PA)", "Samford",
    "San Diego", "South Carolina State", "South Dakota", "South Dakota State",
    "Southeast Missouri State", "Southeastern Louisiana", "Southern",
    "Southern Illinois", "Southern Utah", "St. Thomas (MN)", "Stephen F. Austin",
    "Stetson", "Stonehill", "Stony Brook", "Tarleton State", "Tennessee State",
    "Tennessee Tech", "Texas A&M-Commerce", "Texas Southern", "Towson",
    "UC Davis", "UT Martin", "UTRGV", "Utah Tech", "VMI", "Valparaiso",
    "Villanova", "Wagner", "Weber State", "West Georgia", "Western Carolina", "Western Illinois",
    "William & Mary", "Wofford", "Yale", "Youngstown State",
}

_CAPP_NAMES_LOWER = {name.casefold(): name for name in CAPP_TEAM_NAMES}

NFL_TEAM_NAMES = {
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars",
    "Kansas City Chiefs", "Las Vegas Raiders", "Los Angeles Chargers",
    "Los Angeles Rams", "Miami Dolphins", "Minnesota Vikings",
    "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers",
    "San Francisco 49ers", "Seattle Seahawks", "Tampa Bay Buccaneers",
    "Tennessee Titans", "Washington Commanders",
}

_NFL_NAMES_LOWER = {name.casefold(): name for name in NFL_TEAM_NAMES}

# ============================================================
# Season Week Date Mapping (ported from espn_live.py)
# ============================================================
_SEASON_WEEK_DATES = {
    2026: {
        0:  ("20260822", "20260824"),
        1:  ("20260825", "20260831"),
        2:  ("20260901", "20260907"),
        3:  ("20260908", "20260914"),
        4:  ("20260915", "20260921"),
        5:  ("20260922", "20260928"),
        6:  ("20260929", "20261005"),
        7:  ("20261006", "20261012"),
        8:  ("20261013", "20261019"),
        9:  ("20261020", "20261026"),
        10: ("20261027", "20261102"),
        11: ("20261103", "20261109"),
        12: ("20261110", "20261116"),
        13: ("20261117", "20261123"),
        14: ("20261124", "20261130"),
        15: ("20261201", "20261207"),   # Conference championships
    },
    2025: {
        0:  ("20250823", "20250825"),
        1:  ("20250826", "20250901"),
        2:  ("20250902", "20250908"),
        3:  ("20250909", "20250915"),
        4:  ("20250916", "20250922"),
        5:  ("20250923", "20250929"),
        6:  ("20250930", "20251006"),
        7:  ("20251007", "20251013"),
        8:  ("20251014", "20251020"),
        9:  ("20251021", "20251027"),
        10: ("20251028", "20251103"),
        11: ("20251104", "20251110"),
        12: ("20251111", "20251117"),
        13: ("20251118", "20251124"),
        14: ("20251125", "20251201"),
        15: ("20251202", "20251208"),
    },
    2024: {
        0:  ("20240824", "20240826"),
        1:  ("20240827", "20240902"),
        2:  ("20240903", "20240909"),
        3:  ("20240910", "20240916"),
        4:  ("20240917", "20240923"),
        5:  ("20240924", "20240930"),
        6:  ("20241001", "20241007"),
        7:  ("20241008", "20241014"),
        8:  ("20241015", "20241021"),
        9:  ("20241022", "20241028"),
        10: ("20241029", "20241104"),
        11: ("20241105", "20241111"),
        12: ("20241112", "20241118"),
        13: ("20241119", "20241125"),
        14: ("20241126", "20241202"),
        15: ("20241203", "20241209"),
    },
    2023: {
        0:  ("20230826", "20230828"),
        1:  ("20230829", "20230904"),
        2:  ("20230905", "20230911"),
        3:  ("20230912", "20230918"),
        4:  ("20230919", "20230925"),
        5:  ("20230926", "20231002"),
        6:  ("20231003", "20231009"),
        7:  ("20231010", "20231016"),
        8:  ("20231017", "20231023"),
        9:  ("20231024", "20231030"),
        10: ("20231031", "20231106"),
        11: ("20231107", "20231113"),
        12: ("20231114", "20231120"),
        13: ("20231121", "20231127"),
        14: ("20231128", "20231204"),
        15: ("20231205", "20231211"),
    },
}

_POSTSEASON_DATES = {
    2026: ("20261208", "20270119"),
    2025: ("20251209", "20260115"),
    2024: ("20241210", "20250115"),
    2023: ("20231211", "20240115"),
}

# ============================================================
# Live polling state
# ============================================================
_games_cache = []   # list of game info dicts
_plays_cache = {}   # game_id -> mapped result dict
_lock = threading.Lock()
_poller_thread = None
_poller_started_at = 0.0
_last_poll_started_at = 0.0
_last_poll_completed_at = 0.0
_last_poll_duration_ms = 0.0
_last_poll_error = ""
_initial_poll_done = threading.Event()
# game_id -> {league, last_seen, next_poll, final_since, pre_since}
# Populated by mark_game_active() from client traffic; the ONLY thing the
# poller will ever fetch. Empty dict == zero ESPN calls.
_active_games = {}
_games_cache_at = 0.0   # when the on-demand scoreboard list was last refreshed

# ============================================================
# Team Name Utilities
# ============================================================

def espn_name_to_capp_name(espn_display_name, league="cfb"):
    if not espn_display_name:
        return None
    name = _strip_accents(espn_display_name.strip())
    if league == "nfl":
        if name in NFL_TEAM_NAMES:
            return name
        if name.casefold() in _NFL_NAMES_LOWER:
            return _NFL_NAMES_LOWER[name.casefold()]
        return None
    if name in ESPN_NAME_OVERRIDES:
        return ESPN_NAME_OVERRIDES[name]
    if name.casefold() in _CAPP_NAMES_LOWER:
        return _CAPP_NAMES_LOWER[name.casefold()]
    words = name.split()
    for i in range(len(words) - 1, 0, -1):
        candidate = " ".join(words[:i])
        if candidate.casefold() in _CAPP_NAMES_LOWER:
            return _CAPP_NAMES_LOWER[candidate.casefold()]
        if candidate in ESPN_NAME_OVERRIDES:
            return ESPN_NAME_OVERRIDES[candidate]
    return None

# ============================================================
# Clock Utilities
# ============================================================

# NFL play text uses GAMEBOOK abbreviations, which differ from the abbreviation
# in ESPN's own team data for six teams. Attributing a timeout by looking for
# the team abbreviation in the play text therefore matched nothing for them, and
# both timeout flags stayed "No" - no red scoreboard background and no
# timeouts-remaining decrement. Confirmed still live on 401874101 (Dallas at
# Arizona, Aug 22 2026): "Timeout #2 by ARZ" dropped while "Timeout #1 by DAL"
# registered.
#
# College is unaffected - its play text uses a different format entirely.
GAMEBOOK_ABBREV_ALIASES = {
    "ARI": ["arz"],     # Arizona Cardinals
    "BAL": ["blt"],     # Baltimore Ravens
    "CLE": ["clv"],     # Cleveland Browns
    "HOU": ["hst"],     # Houston Texans
    "LAR": ["la"],      # Los Angeles Rams
    "WSH": ["was"],     # Washington Commanders
}


def _timeout_abbrevs(team_abbrev):
    """
    Every abbreviation a team's timeout might be written under, lowercased.
    The real abbreviation comes first so an alias can never beat a genuine match.
    """
    if not team_abbrev:
        return []
    real = team_abbrev.strip()
    out = [real.lower()]
    out.extend(GAMEBOOK_ABBREV_ALIASES.get(real.upper(), []))
    return out


def _fold_accents(s):
    """
    Accent-fold a string for comparison against play text.

    ⚠ Required for correctness, not a nicety. CAPP's canonical team name is plain
    ASCII ("San Jose State" — the whole layer stack was deliberately folded to
    ASCII), but ESPN's PLAY TEXT still carries the accent ("Timeout San José
    State"). The substring test therefore never matched and every San José State
    timeout was silently dropped — 6 of 8 in SJSU @ USC, Aug 29 2026, while USC's
    own matched only because its abbreviation happens to be "USC".

    Folding both sides is generic: it fixes any future accented team and cannot
    change the result for a team that already matched.
    """
    return "".join(c for c in unicodedata.normalize("NFKD", s or "")
                   if not unicodedata.combining(c))


def _abbrev_in_text(abbrevs, desc_lower):
    """
    Match an abbreviation as a WHOLE WORD, never as a substring.

    ⚠ This is not a refinement, it is required for correctness. The Rams alias
    is "LA", and a plain substring test matches it inside "LAC" - so a Chargers
    timeout in a Rams home game would be credited to the Rams. It also matches
    inside ordinary words ("Atlanta", "delay"). Word boundaries remove the whole
    class of false positive, for the real abbreviations as well as the aliases.
    """
    for a in abbrevs:
        if a and re.search(r"\b" + re.escape(a) + r"\b", desc_lower):
            return True
    return False


def _clock_to_seconds(clock_str):
    try:
        parts = clock_str.split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
    except (ValueError, AttributeError):
        pass
    return 0

def _seconds_to_clock(seconds):
    if seconds < 0:
        seconds = 0
    return f"{seconds // 60}:{seconds % 60:02d}"

_PLAY_DURATION = {
    "rush": 7, "pass reception": 8, "pass incompletion": 5,
    "passing touchdown": 8, "rushing touchdown": 7, "punt": 10,
    "field goal good": 5, "field goal missed": 5, "blocked field goal": 5,
    "blocked punt": 5, "sack": 7, "penalty": 6, "fumble recovery": 7,
    "interception": 7, "pass interception return": 7,
    "kickoff": 5, "kickoff return": 5, "timeout": 0, "officials time out": 0,
}
_DEFAULT_DURATION = 6

def _estimate_play_duration(play_type_text):
    lower = play_type_text.lower()
    for key, dur in _PLAY_DURATION.items():
        if key in lower:
            return dur
    return _DEFAULT_DURATION

def estimate_snap_clocks(plays):
    if not plays:
        return
    prev_period = None
    prev_espn_secs = 900
    for play in plays:
        period = play.get("period", 1)
        espn_secs = _clock_to_seconds(play.get("clock", "0:00"))
        if period != prev_period:
            prev_period = period
            prev_espn_secs = 900
        duration = _estimate_play_duration(play.get("play_type_text", ""))
        snap_secs = espn_secs + duration
        if snap_secs > prev_espn_secs:
            snap_secs = prev_espn_secs
        if snap_secs > 900:
            snap_secs = 900
        play["clock"] = _seconds_to_clock(snap_secs)
        prev_espn_secs = espn_secs

_TEXT_SNAP_RE = re.compile(r"^\s*\((\d{1,2}):(\d{2})\)")
# A timeout line carries its own time: "Timeout Air Force, clock 07:52".
_TEXT_TO_RE = re.compile(r"^\s*(?:officials\s+)?timeout\b[^()]*?clock\s+(\d{1,2}):(\d{2})", re.I)
# The end time a crew writes into a scoring line: "... TOUCHDOWN, clock 00:59".
_TEXT_END_RE = re.compile(r"\bclock\s+(\d{1,2}):(\d{2})", re.I)
# The two-point try as the crew writes it at the END of the touchdown line: "... TOUCHDOWN, clock 05:58, 1ST DOWN #7
# J.Wright rush attempt failed" / "#3 L.Brooks pass attempt Successful" / "(Gio Lopez Run for Two-Point Conversion)".
# A kick try never reads "rush/pass attempt" (Sep 14 2026, 125 games checked).
_TWO_POINT_TRY_TEXT = re.compile(r"\b(?:rush|pass|run)\s+attempt\s+(?:failed|successful)\b|\btwo[- ]point\b", re.I)


def _typed_secs(play):
    """The time the crew typed into a play's text - its "(MM:SS)" snap, or a timeout's "clock MM:SS" - or None."""
    d = str(play.get("description") or "")
    m = _TEXT_SNAP_RE.match(d) or _TEXT_TO_RE.match(d)
    if not m:
        return None
    s = int(m.group(1)) * 60 + int(m.group(2))
    return s if s <= 15 * 60 else None


def reslot_late_timeouts(plays):
    """Move a timeout the stat crew typed in LATE back to where its typed time belongs. Returns the moves made.

    ⚠ Sep 12 2026, SMU Q2: "Timeout UC Davis, clock 06:36" was entered at halftime and ESPN numbered it after the
    last kneel-down (0:24); "Timeout UC Davis, clock 02:00" was entered 9 minutes late and numbered after SMU's 0:54
    timeout. Ordered by sequence number they sat at the end of the quarter and the clock ran backwards
    (0:24 -> 6:36). NCAA had both in the right place - the typed times were right, the sequence was not.
    A timeout is only moved when it is plainly out of place: the nearest earlier play with a typed time has a time
    BELOW the timeout's. It goes in front of the first play in the quarter typed below its time. Only timeouts move -
    never plays - and only within their quarter.
    """
    moves = []
    i = 0
    while i < len(plays):
        t = plays[i]
        d = str(t.get("description") or "")
        tsecs = _typed_secs(t) if _TEXT_TO_RE.match(d) else None
        if tsecs is None:
            i += 1
            continue
        period = t.get("period", 1)
        prev = next((plays[k] for k in range(i - 1, -1, -1)
                     if plays[k].get("period", 1) == period and _typed_secs(plays[k]) is not None), None)
        # MEASURED Sep 12: crews type a timeout ONE second above the play it follows ("(07:54)" then "Timeout UC Davis,
        # clock 07:55") - that timeout is already in the right place. The ones typed in late sat 66s and 372s out of
        # place. Only more than 5 seconds counts as out of place.
        if prev is None or _typed_secs(prev) + 5 >= tsecs:
            i += 1
            continue
        target = next((k for k in range(i) if plays[k].get("period", 1) == period
                       and _TEXT_SNAP_RE.match(str(plays[k].get("description") or ""))
                       and _typed_secs(plays[k]) < tsecs), None)
        if target is None:
            i += 1
            continue
        plays.insert(target, plays.pop(i))
        moves.append({"text": d[:60], "period": period, "from": i, "to": target})
        i += 1
    return moves


def reslot_timeouts_by_clock(entries):
    """reslot_late_timeouts again, on the FINISHED rows, by each row's clock. Returns the moves made.

    ⚠ WHY A SECOND PASS (Syracuse @ Pitt Q4, Sep 17-18 2026, Roger live). reslot_late_timeouts runs on the raw feed,
    before any clock work. There it saw two Syracuse timeouts (3:03, 2:56) sitting above five plays the crew had
    stamped (02:00) - in order, as far as the typed times could tell. The backup check then put the five plays back
    on their real clocks (3:08 / 3:03 / 2:56 / 2:56 / 2:15) and the same two timeouts now read backwards against
    them. Roger: "166-169 are now out of order". Same rule, same limits - only a timeout moves, only inside its
    quarter, only when plainly out of place (more than 5 s) - just measured against the corrected clocks, so a
    timeout the crew filed early lands where its own typed time says, after the play with the higher clock and
    before the first play with a lower one (CBS's order for the Pitt pair).
    """
    moves = []
    i = 0
    while i < len(entries):
        t = entries[i]
        if not (str(t.get("play_text") or "").strip().lower().startswith("timeout")
                or t.get("home_time_out") == "Yes" or t.get("away_time_out") == "Yes"):
            i += 1
            continue
        tsecs = _clock_to_seconds(str(t.get("clock") or ""))
        q = t.get("quarter")
        if not tsecs or q is None:
            i += 1
            continue

        def _play_secs(e):
            # A play nobody vouches for (the backup check paired nothing to it) cannot anchor a timeout either
            # way - Pitt's second kickoff line, stamped 2:07 and unpaired, sat between the two timeouts and the
            # corrected plays and hid the whole problem. Same principle as the stuck-run fence in cbs_check.
            if e.get("quarter") != q or str(e.get("down") or "") == "OTO" or e.get("home_time_out") == "Yes" \
                    or e.get("away_time_out") == "Yes" or str(e.get("play_text") or "").strip().lower().startswith("timeout") \
                    or e.get("ncaa_status") == "unverified":
                return None
            s = _clock_to_seconds(str(e.get("clock") or ""))
            return s if s else None

        prev = next((_play_secs(entries[k]) for k in range(i - 1, -1, -1)
                     if entries[k].get("quarter") == q and _play_secs(entries[k]) is not None), None)
        nxt = next((_play_secs(entries[k]) for k in range(i + 1, len(entries))
                    if entries[k].get("quarter") == q and _play_secs(entries[k]) is not None), None)
        if prev is not None and prev + 5 < tsecs:
            # Filed LATE: the play before it already shows a lower clock. In front of the first play below its time.
            target = next((k for k in range(i) if entries[k].get("quarter") == q
                           and _play_secs(entries[k]) is not None and _play_secs(entries[k]) < tsecs), None)
            if target is None:
                i += 1
                continue
            entries.insert(target, entries.pop(i))
            moves.append({"text": str(t.get("play_text") or "")[:60], "quarter": q, "from": i, "to": target})
            i += 1
            continue
        if nxt is not None and nxt > tsecs + 5:
            # Filed EARLY: the play after it still shows a HIGHER clock - the Pitt case, once the five plays under the
            # two Syracuse timeouts went from 2:00 back to 3:08-2:15. It goes after every later play with a higher
            # clock, in front of the first at or below its time (a timeout stops the clock at T and the next snap is at
            # T - Roger's measured convention: the timeout carries the clock of the play after it).
            target = next((k for k in range(i + 1, len(entries)) if entries[k].get("quarter") != q
                           or (_play_secs(entries[k]) is not None and _play_secs(entries[k]) <= tsecs)), len(entries))
            row = entries.pop(i)
            entries.insert(target - 1, row)
            moves.append({"text": str(row.get("play_text") or "")[:60], "quarter": q, "from": i, "to": target - 1})
            continue                              # re-check the row now at i
        i += 1
    return moves


def apply_text_snap_clocks(plays):
    """The snap time the stat crew typed into the play text is the clock. Final say.

    ⚠ WHY (Sep 12-13 2026). ESPN's clock FIELD is often the clock after the play,
    or frozen for a whole run (Fresno: five plays at 7:36), and our estimates on top
    of it drifted 3-7s on ~60 Air Force plays. Worse, fix_clock_anomalies assumes the
    clock never goes up inside a quarter, so ONE misfiled play (SMU Q2) dragged every
    later clock down to its time. Most ESPN play lines start with the real snap -
    "(09:08) Shotgun ..." - so:
      1. a play whose text carries a snap time gets exactly that clock, and is never
         moved by anything after this;
      2. a play without one follows Roger's order below: ESPN's own clock when it moved,
         then NCAA, then an even spread between trusted clocks (never a copied, stuck clock).
    Returns the number of clocks changed.
    """
    changed = 0
    for play in plays:
        _desc = str(play.get("description") or "")
        m = _TEXT_SNAP_RE.match(_desc) or _TEXT_TO_RE.match(_desc)
        if not m:
            play["_snap_from_text"] = False
            continue
        secs = int(m.group(1)) * 60 + int(m.group(2))
        if secs > 15 * 60:
            play["_snap_from_text"] = False
            continue
        # ⚠ A play cannot end AFTER it snapped. Crews type a touchdown's snap one second below its own end time:
        # "(00:58) ... TOUCHDOWN, clock 00:59" (Sep 12: SMU x4, Air Force x1), and the extra point + kickoff that
        # follow at 0:59 then make the clock run UP. Roger, Sep 13 2026: use the end time - "that seems closer to
        # what really happened" (Bleacher Report had that SMU touchdown at 1:05; no source has the real snap).
        _end = _TEXT_END_RE.search(_desc)
        if _end and _TEXT_SNAP_RE.match(_desc):
            end_secs = int(_end.group(1)) * 60 + int(_end.group(2))
            if secs < end_secs <= 15 * 60:
                secs = end_secs
        new = _seconds_to_clock(secs)
        if play.get("clock") != new:
            changed += 1
        play["clock"] = new
        play["_snap_from_text"] = True
        play["_clock_src"] = "text"

    # ⚠ Roger, Sep 13 2026 - the order for every clock: "The Snap Time should be used when its there..
    # the first one after that should be ESPNs Feed, If that is Messed up its NCAA and if that doesnt
    # work its Best Guess" - and stuck clocks are fixed under the same rule.
    #
    # STUCK CLOCKS. ESPN often leaves its clock field at the last time the crew typed (Fresno, Sep 12:
    # a kickoff and five plays all "15:00", then six all "10:27"). Taking that as-is gave 27 stuck
    # runs / 139 plays. So ESPN's clock only counts when it fits (not above the trusted clock before
    # it, not below the next text time) and is not the THIRD play in a row on one clock.
    # Anything else is "messed up".
    # NCAA (3) is not in this pipeline yet - it comes with the NCAA check.
    # BEST GUESS (4): plays still without a trusted clock are spread evenly between the trusted
    # clocks on either side - never copied from a neighbour, which is what made clocks stick.
    for period in {p.get("period", 1) for p in plays}:
        idx = [i for i, p in enumerate(plays) if p.get("period", 1) == period]
        if not idx:
            continue
        if period <= 4:
            start = 15 * 60
        else:
            start = max((_clock_to_seconds(plays[i].get("_espn_clock") or "0:00") for i in idx), default=0)
            if start <= 0:
                continue                                    # untimed overtime - leave it
        text_at = [(n, _clock_to_seconds(plays[i]["clock"])) for n, i in enumerate(idx) if plays[i]["_snap_from_text"]]
        # Two plays in a row on the same clock is normal (false start, touchback, incomplete pass);
        # a THIRD on the same clock is a stuck feed clock. `same` counts trusted plays at `last`.
        known = {}
        last, same = start, 0
        for n, i in enumerate(idx):
            p = plays[i]
            if p["_snap_from_text"]:
                s = _clock_to_seconds(p["clock"])
                same = same + 1 if s == last else 1
                known[i] = last = s
                continue
            is_ko = "kickoff" in str(p.get("play_type_text") or "").lower()
            if is_ko and n == 0:
                known[i] = last = start                     # opening kickoff of the period
                same = 1
                p["_clock_src"] = "kickoff"
                continue
            if is_ko:
                # a kickoff after a score starts when the score ended: "... TOUCHDOWN, clock 04:12"
                m_end = re.search(r"clock\s+(\d{1,2}):(\d{2})", str(plays[idx[n - 1]].get("description") or ""), re.I)
                if m_end:
                    s = int(m_end.group(1)) * 60 + int(m_end.group(2))
                    if s <= last:
                        same = same + 1 if s == last else 1
                        known[i] = last = s
                        p["_clock_src"] = "kickoff"
                        continue
            raw = p.get("_espn_clock")
            if raw:
                rs = _clock_to_seconds(raw)
                lo = next((s for nn, s in text_at if nn > n), 0)
                if lo <= rs < last or (rs == last and same < 2):
                    same = same + 1 if rs == last else 1
                    known[i] = last = rs
                    p["_clock_src"] = "espn"
        for i, s in known.items():                          # trusted clocks go on the play
            new = _seconds_to_clock(s)
            if plays[i]["clock"] != new:
                plays[i]["clock"] = new
                changed += 1
        k = 0
        while k < len(idx):
            if idx[k] in known:
                k += 1
                continue
            j = k
            while j < len(idx) and idx[j] not in known:
                j += 1
            hi = known[idx[k - 1]] if k > 0 else start
            gap = idx[k:j]
            lo = min(known[idx[j]], hi) if j < len(idx) else 0     # nothing trusted after: spread toward 0:00
            new_secs = [round(hi - (hi - lo) * t / (len(gap) + 1)) for t in range(1, len(gap) + 1)]
            for i, s in zip(gap, new_secs):
                new = _seconds_to_clock(s)
                plays[i]["_clock_src"] = "guess"
                if plays[i]["clock"] != new:
                    plays[i]["clock"] = new
                    changed += 1
            k = j
    return changed


def fix_clock_anomalies(plays, default_elapsed=30, min_streak=6):
    if len(plays) < min_streak:
        return
    n = len(plays)
    i = 0
    while i < n:
        period = plays[i].get("period", 1)
        clock_val = plays[i].get("clock", "0:00")
        start_secs = _clock_to_seconds(clock_val)
        j = i + 1
        while (j < n
               and plays[j].get("period") == period
               and plays[j].get("clock") == clock_val):
            j += 1
        streak_len = j - i
        if streak_len >= min_streak:
            end_secs = None
            for k in range(j, n):
                if plays[k].get("period") != period:
                    break
                candidate = _clock_to_seconds(plays[k].get("clock", "0:00"))
                if candidate < start_secs:
                    end_secs = candidate
                    break
            if end_secs is not None:
                total_gap = start_secs - end_secs
                step = total_gap / streak_len
                for idx in range(1, streak_len):
                    new_secs = int(start_secs - step * idx)
                    plays[i + idx]["clock"] = _seconds_to_clock(max(new_secs, 0))
            else:
                for idx in range(1, streak_len):
                    new_secs = start_secs - default_elapsed * idx
                    plays[i + idx]["clock"] = _seconds_to_clock(max(new_secs, 0))
        i = j
    prev_period = None
    prev_secs = 900
    for play in plays:
        period = play.get("period", 1)
        clock_secs = _clock_to_seconds(play.get("clock", "0:00"))
        if period != prev_period:
            prev_period = period
            prev_secs = 900
        if clock_secs > prev_secs:
            play["clock"] = _seconds_to_clock(prev_secs)
            clock_secs = prev_secs
        prev_secs = clock_secs

# ============================================================
# Field Position
# ============================================================

def convert_field_position(yards_to_endzone):
    if yards_to_endzone is None:
        return 0
    if yards_to_endzone > 50:
        return -(100 - yards_to_endzone)
    elif yards_to_endzone < 50:
        return yards_to_endzone
    else:
        return -50

def fill_missing_field_positions(entries):
    prev_fp = 0
    prev_gain = 0
    for entry in entries:
        fp = entry.get("field_position", 0)
        # A STOPPAGE ROW IS TRANSPARENT HERE (fixed Sep 16 2026). An officials timeout has no field position at all -
        # map_espn_play writes "" for it - so it must not become prev_fp. It used to, and then the next row with no
        # field position of its own reached `prev_fp < 0` and compared str to int: TypeError, the /plays route 500s,
        # and the coach gets NO DATA for the whole game.
        # MEASURED: NFL week 1, 2 of 16 games. It needs the two-minute warning (or an official timeout) followed
        # IMMEDIATELY by a team timeout, which is why the other 14 - and 15 of 15 college games - never tripped it:
        #   NE@SEA  idx 164 "Two-Minute Warning" (fp "") -> idx 165 "Timeout #2 by SEA" (fp 0)
        #   SF@LAR  idx 154 "Official Timeout"   (fp "") -> idx 155 "Timeout #2 by SF"  (fp 0)
        # Skipping the stoppage fills that timeout row from the last real snap instead: SF@LAR gets 47, which is
        # exactly where the feed puts the next snap. No game that survived before can change - any game reaching
        # this branch crashed.
        if not isinstance(fp, int):
            continue
        if fp == 0 and prev_fp != 0:
            if prev_fp < 0:
                prev_yte = 100 + prev_fp
            else:
                prev_yte = prev_fp
            new_yte = max(0, min(100, prev_yte - prev_gain))
            entry["field_position"] = convert_field_position(new_yte)
        prev_fp = entry.get("field_position", 0)
        try:
            prev_gain = int(entry.get("gain", 0) or 0)
        except (TypeError, ValueError):
            prev_gain = 0          # same class of bug: a non-numeric gain must never 500 a whole game

# ============================================================
# Scoreboard Lag
# ============================================================

def apply_scoreboard_lag(entries, initial_home=0, initial_away=0):
    prev_home = initial_home
    prev_away = initial_away
    for entry in entries:
        curr_home = entry["home_score"]
        curr_away = entry["away_score"]
        entry["home_score"] = prev_home
        entry["away_score"] = prev_away
        prev_home = curr_home
        prev_away = curr_away
    return prev_home, prev_away

# ============================================================
# Post-Lag Auto-Fix Pass
# ============================================================

def _auto_fix_entries(entries):
    """
    Post-mapping, post-scoreboard-lag auto-fix pass.

    Runs AFTER apply_scoreboard_lag(), BEFORE _qc_flag_entries().
    Detects and corrects errors that survived the pre-mapping pipeline.
    Modifies entries in-place.

    Fixes applied
    -------------
    1. EP/2PT row score regression (wrong-team EP)
       After lag, an EP row where one team's score is LOWER than the
       previous row means the EP value was subtracted from the wrong
       team in map_espn_play.  We reverse the regression and credit
       the correct team.  This catches any defensive/special-teams TD
       cases where _annotate_td_scoring_teams still missed the scorer.

    2. Score regression on non-EP rows
       A negative delta on a regular play row cannot be safely
       auto-corrected without knowing the true score — flagged only.

    Returns {index: "fix description"} for entries that were corrected.
    """
    fixes = {}

    for i in range(1, len(entries)):
        entry = entries[i]
        prev  = entries[i - 1]
        down  = str(entry.get("down", ""))

        # ── Fix 1: EP/2PT row score regression ───────────────────────────
        # One team's displayed score went DOWN entering an EP/2PT play.
        # That means the EP was subtracted from the wrong side earlier.
        # Reverse the regression: restore the decreased team, reduce the
        # other team by the same amount.
        if down in ("EP", "2PT"):
            hd = entry["home_score"] - prev["home_score"]
            ad = entry["away_score"] - prev["away_score"]
            if hd < 0 <= ad:
                # Home score wrongly reduced — give it back, take from away
                correction = abs(hd)
                entry["home_score"] += correction
                entry["away_score"] -= correction
                fixes[i] = f"Auto-fixed: EP credited to wrong team (home restored +{correction})"
            elif ad < 0 <= hd:
                # Away score wrongly reduced — give it back, take from home
                correction = abs(ad)
                entry["away_score"] += correction
                entry["home_score"] -= correction
                fixes[i] = f"Auto-fixed: EP credited to wrong team (away restored +{correction})"

    return fixes


# ============================================================
# The score ESPN itself publishes (scoringPlays)
# ============================================================

def score_events(data):
    """ESPN's own scoring summary as (period, clock seconds, home, away), in game order.

    ⚠ THE AUTHORITY ON THE SCORE, AND WE WERE NOT READING IT. ESPN's play rows lag: a field goal at the end of a half
    leaves every row of the next period carrying the old score until ESPN's own numbers catch up. MEASURED on
    Syracuse @ Pitt (Sep 17 2026, Roger live): Syracuse kicked a 42-yarder at Q2 0:02 to go to 10, and the first TEN
    Q3 rows still read 17-7, so ten boards would have gone to film 3 points short. `_auto_fix_entries` says a score
    regression "cannot be safely auto-corrected without knowing the true score" - this IS that true score, and it
    arrives in the same summary payload we already download (10 events on the SMU game, with the resulting score on
    each one).
    """
    out = []
    for sp in (data.get("scoringPlays") or []):
        q = (sp.get("period") or {}).get("number")
        raw = str((sp.get("clock") or {}).get("displayValue") or "")
        h, a = sp.get("homeScore"), sp.get("awayScore")
        if q is None or h is None or a is None or not re.match(r"^\d{1,2}:\d{2}$", raw):
            continue
        try:
            out.append((int(q), _clock_to_seconds(raw), int(h), int(a)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: (x[0], -x[1]))
    return out


def _raise_lagging_scores(entries, events):
    """Lift any row sitting below a score ESPN's summary says was already on the board.

    RAISE ONLY, never lower - a score CAN be wrong high in ESPN's rows too, but which of the two numbers is wrong
    cannot be told from the feed, and lowering a score that really was earned would put a wrong number on film.

    Only a row STRICTLY LATER in game time than the scoring event is touched, which keeps two conventions intact:
      - a scoring row shows the score BEFORE its own points (that is what `apply_scoreboard_lag` builds), and
      - the extra point and the kickoff that share the scoring play's clock keep the scores they already carry.

    One note per RUN of corrected rows, not per row: a qc_issue on every one of ten rows would push that quarter's
    issue share over 10% and set off the backup prompt for a problem that had just been fixed.
    """
    fixes = {}
    if not events:
        return fixes
    in_run = False
    for i, e in enumerate(entries):
        raw = str(e.get("clock") or "")
        try:
            q = int(e.get("quarter"))
        except (TypeError, ValueError):
            in_run = False
            continue
        if not re.match(r"^\d{1,2}:\d{2}$", raw):
            in_run = False
            continue
        c = _clock_to_seconds(raw)
        floor_h = floor_a = None
        for (eq, ec, eh, ea) in events:
            if (eq, -ec) < (q, -c):
                floor_h, floor_a = eh, ea
            else:
                break
        h, a = e.get("home_score"), e.get("away_score")
        if floor_h is None or h is None or a is None:
            in_run = False
            continue
        if h < floor_h or a < floor_a:
            was = f"{h}-{a}"
            e["home_score"], e["away_score"] = max(h, floor_h), max(a, floor_a)
            if not in_run:
                note = f"Auto-fixed: score {was} → {e['home_score']}-{e['away_score']}"
                fixes[i] = note
                # ⚠ ON THE ROW, not by index: `_fill_scoring_gaps` can insert rows after this pass, and
                # `_qc_flag_entries`' own result is written over qc_issue later, so the note is carried on the entry
                # and merged in at that point.
                e["_score_fix_note"] = note
                in_run = True
        else:
            in_run = False
    return fixes


# A scoring play, an extra point / try, or a kickoff - in ESPN's typed style ("... for 14 yards, TOUCHDOWN",
# "field goal attempt from 30 yards GOOD", "#99 N.Reed kickoff 65 yards") and in its scoring-summary style
# ("Dominique Henry 13 Yd pass from Caden Veltkamp (Connor Cook Kick)", "Connor Calvert 39 Yd Field Goal").
_SCORE_TEXT = re.compile(r"touchdown|field goal|kick attempt|extra point|safety|two.point|kickoff|\b\d+ yd\b|for a td\b",
                         re.I)


def _lower_spiked_scores(entries, events):
    """Bring down a run of rows showing a score the game had not reached yet.

    The case (SMU vs UC Davis Q2, Sep 12 2026, found on Roger's Sep 18 replay): the crew deleted and re-typed two plays
    from the 7:00-5:30 stretch late in the half, and each re-entry took the clock and score OF THAT MOMENT (0:59 /
    35-0, 0:24 / 42-0). `apply_scoreboard_lag` hands every row the score of the row before it, so the four rows after
    those two re-entries read 35-0 and 42-0 while the game was 28-0 - four wrong boards - and the two rows AFTER
    them were flagged "Score dropped -14" for being right. `_raise_lagging_scores` is raise-only and cannot touch it.

    A SPIKE, per side, needs all three:
      1. the score RISES at an ordinary row - a play, a penalty line, a timeout - never at a scoring play, an extra
         point, a try or a kickoff, where a rise is what should happen;
      2. the run then COMES BACK DOWN below its lowest score (ESPN's rows contradict themselves, and a score never
         goes down, so the run is the wrong part);
      3. the row sits ABOVE the score ESPN's scoring summary allows at its clock (the last event at or before it,
         strictly before it for the scoring row itself). A row the summary vouches for is never touched, whatever
         the rows after it say - they may be the lagging ones, stuck on a clock the raise could not lift.
    The value is that summary ceiling, held between the score entering the run and the score after it; when the
    summary is behind those neighbours its clock is stale and the score after the run stands.

    ⚠ MEASURED on the way here (Sep 12 corpus): the first version ("above the ceiling and later rows lower") lowered
    218 rows in 63 games and got four wrong AT scoring plays - an extra point 36 s off the summary's clock sent to
    0-0 (Texas State), a kickoff typed 5:00 after a TD the summary has at 4:59 sent back to the score before the TD
    (Army), a "0:00" summary clock lowering a real touchdown (Nicholls); rule 1 leaves every one alone. Without
    rule 3, HCU vs Arkansas Baptist Q2 lowered five rows at 28-0 the summary vouched for, because the rows after them
    sit on a stuck clock and still read 21-0. One note per run, like the raise.
    """
    fixes = {}
    if not events:
        return fixes

    def _secs(e):
        raw = str(e.get("clock") or "")
        return _clock_to_seconds(raw) if re.match(r"^\d{1,2}:\d{2}$", raw) else None

    def _q(e):
        try:
            return int(e.get("quarter"))
        except (TypeError, ValueError):
            return None

    def _is_scoring_row(e):
        down = str(e.get("down") or "").strip().upper()
        if down in ("EP", "2PT", "KO"):
            return True
        return bool(_SCORE_TEXT.search(str(e.get("play_text") or "")))

    def _ceiling(e, side, strict):
        q, c = _q(e), _secs(e)
        if q is None or c is None:
            return None
        val = 0
        for (eq, ec, eh, ea) in events:
            key = (eq, -ec)
            if key < (q, -c) or (not strict and key == (q, -c)):
                val = eh if side == "home_score" else ea
            else:
                break
        return val

    # Rows with a usable score, in table order (a negative score is a broken row - not evidence either way).
    order = [i for i, e in enumerate(entries)
             if isinstance(e.get("home_score"), int) and isinstance(e.get("away_score"), int)
             and e["home_score"] >= 0 and e["away_score"] >= 0]
    original = {i: (entries[i]["home_score"], entries[i]["away_score"]) for i in order}
    for side in ("home_score", "away_score"):
        n = 0
        while n + 1 < len(order):
            i_prev, i = order[n], order[n + 1]
            before, s = entries[i_prev][side], entries[i][side]
            if not (s > before) or _is_scoring_row(entries[i]):
                n += 1
                continue
            # A rise at an ordinary row. The row after an extra point or a kickoff rises too - the scoring row shows
            # the score BEFORE its points, so the points land on the next row - which is why rule 3 is asked here as
            # well: the run may only start on a row the summary says is too high (Purdue Q1: the row after the
            # 7-6 extra point read 7-7, and a run started there swallowed the rest of the quarter).
            ceil_i = _ceiling(entries[i], side, strict=False)
            if ceil_i is None or s <= ceil_i:
                n += 1
                continue
            # Rule 1 met. Walk the run while the score stays at or above its lowest value.
            run, low, m = [i], s, n + 2
            after = None
            while m < len(order):
                j = order[m]
                sj = entries[j][side]
                if sj < low:
                    after = sj
                    break
                run.append(j)
                low = min(low, sj)
                m += 1
            if after is None:
                n = m                              # never came back down: a real score, leave it
                continue
            # Rule 2 met. A score entering the run that is HIGHER than the one after it is a spike of its own (Purdue:
            # a stray 13-20 row sat right before the 23-23 stretch) - the lower neighbour is the one to hold to.
            before = min(before, after)
            noted = False                          # rule 3 is per row
            for k in run:
                e = entries[k]
                if str(e.get("down") or "").strip().upper() in ("EP", "2PT", "KO"):
                    continue                       # a kick or kickoff inside the run keeps its score: its clock is the
                                                   # scoring play's, and the summary's clock for that is seconds off
                                                   # (Central Arkansas 8:58, Nicholls 8:59: the extra point was
                                                   # being sent to the score before its own touchdown)
                ceil = _ceiling(e, side, strict=bool(_SCORE_TEXT.search(str(e.get("play_text") or ""))))
                if ceil is None:
                    target = before                # no clock to ask the summary about: the score entering the run
                elif e[side] <= ceil:
                    continue                       # the summary vouches for this row - not ours to touch
                elif ceil < before:
                    target = after                 # the summary's clock is stale here; the rows around it decide
                else:
                    target = min(ceil, after)
                if e[side] > target:
                    e[side] = target
                    oh, oa = original[k]
                    note = f"Auto-fixed: score {oh}-{oa} → {e['home_score']}-{e['away_score']}"
                    if e.get("_score_fix_note", "").startswith("Auto-fixed: score ") and k in fixes:
                        fixes[k] = e["_score_fix_note"] = note   # the other side already noted this row: refresh it
                        noted = True
                    elif not noted:
                        fixes[k] = e["_score_fix_note"] = note
                        noted = True
            n = m
    return fixes


# ============================================================
# Scoring Gap Detection (period boundaries)
# ============================================================

def _fill_scoring_gaps(entries, home_display, away_display, events=None):
    """
    After scoreboard lag: detect period-opening KOs where the score
    jumped vs. the previous period's last entry, meaning one or more
    scoring plays were omitted from the feed entirely.

    We insert a single generic placeholder entry so the operator can
    see the gap and enter the correct play(s) manually.  We deliberately
    do NOT assert what type of play occurred (TD, FG, etc.) because the
    score delta alone is not reliable enough — ESPN sometimes also has
    its own score errors layered on top of the real gap, making the
    delta misleading.

    The placeholder entry:
      - Is flagged red (qc_issue set) so it stands out in SBENTRY
      - Shows the score BEFORE the gap (period-ending score)
      - Reports the observed delta so the operator knows what to look for
      - Leaves the KO's lag score unchanged (operator should verify it too)

    Handles any non-zero positive delta. Modifies entries in-place.
    """
    _GAP_QC    = "Manual entry required — scoring play(s) missing from feed"
    gaps_found = 0
    i = 1
    while i < len(entries):
        entry = entries[i]
        prev  = entries[i - 1]

        # Only period-opening KO entries (quarter changes)
        if str(entry.get("down", "")) != "KO":
            i += 1
            continue
        if str(entry.get("quarter", "")) == str(prev.get("quarter", "")):
            i += 1
            continue

        # After lag: entry[i] shows last-period end score;
        # entry[i+1] shows the post-KO score (may include missing scoring).
        if i + 1 >= len(entries):
            i += 1
            continue

        nxt = entries[i + 1]
        dh = nxt["home_score"] - entry["home_score"]
        da = nxt["away_score"] - entry["away_score"]

        # Only act on positive deltas — a score can never legitimately drop
        if dh <= 0 and da <= 0:
            i += 1
            continue

        # ⚠ NOT A GAP IF THE PLAY IS ALREADY ON THE TABLE. MEASURED on Sep 12's 125 games: this check produced 9
        # placeholders and every one was a +3 field goal at the end of a half, with the field-goal row sitting
        # directly above the placeholder (Kentucky, Ole Miss, Memphis, Florida Atlantic, Louisiana-Monroe, Bryant,
        # Liberty, Butler, Samford). Nothing was missing in any of them. The cause is a convention, not an absence: a
        # scoring row carries the score BEFORE it scores, and the next period's kickoff lags at the old score too, so
        # the points only surface a row or two later and the delta reads as an omission. Roger, Sep 17 2026 on the one
        # in his Pitt game: "is it something that we have already fixed and the bookkeeping row Is just noise we dont
        # need?" - it was noise. So: if ESPN's own scoring summary has an event in the period that just ended whose
        # resulting score is the score the game settles on after the boundary, the play is there. Say nothing.
        if events:
            prev_q = None
            try:
                prev_q = int(prev.get("quarter"))
            except (TypeError, ValueError):
                prev_q = None
            settled = (nxt["home_score"], nxt["away_score"])
            if prev_q is not None and any((eq == prev_q and (eh, ea) == settled)
                                          for (eq, _ec, eh, ea) in events):
                i += 1
                continue

        gaps_found += 1

        # Build a human-readable delta string for the operator
        parts = []
        if dh > 0:
            parts.append(f"{home_display} +{dh}")
        if da > 0:
            parts.append(f"{away_display} +{da}")
        delta_str = ", ".join(parts)

        prev_qtr = str(prev.get("quarter", entry.get("quarter", "1")))

        gap_entry = {
            "quarter":        prev_qtr,
            "clock":          "0:00",
            "down":           "?",
            "distance":       0,
            "field_position": 0,
            "gain":           0,
            "home_score":     entry["home_score"],   # score before the gap
            "away_score":     entry["away_score"],
            "possession":     "",
            "home_time_out":  "No",
            "away_time_out":  "No",
            "run_clock":      "No",
            "play_text":      f"Score gap at period boundary ({delta_str}) — enter missing play(s) manually",
            "wallclock":      "",
            "qc_issue":       _GAP_QC,
        }

        # Insert the placeholder before the KO
        entries.insert(i, gap_entry)

        # Skip past the placeholder and the KO; let _qc_flag_entries
        # handle whatever score anomalies remain after the KO
        i += 2

    return gaps_found


# ============================================================
# QC Flagging
# ============================================================

_QC_VALID_POS    = {0, 1, 2, 3, 6, 7, 8}   # valid positive score deltas
_QC_BUNDLED_ART  = {-7, -8}                 # lag mirrors of bundled TD+EP — skip
_QC_STUCK_THRESH = 3   # matches cbs_check.STUCK_RUN (4 until Sep 18 2026 - see the note there)

TIMEOUT_UNVERIFIED_NOTE = "Timeout not verified - check who it is charged to"

# "(14:40) PENALTY SJSU False Start (#9 J.Nix) 3 yards from SJSU07 to SJSU04. NO PLAY" - the whole row is a dead-ball
# foul: no snap is described, so no game clock ran and the next play keeps the same clock. NOT the same as a penalty
# that wiped a play which WAS run ("(13:05) Shotgun ... NO PLAY. ..."), where the row describes the snap first - that
# one still counts toward a stalled crew.
_DEAD_BALL_PENALTY = re.compile(r"^\s*(?:\(\d{1,2}:\d{2}\)\s*)?PENALTY\b", re.I)


def _is_dead_ball_penalty(entry):
    text = str(entry.get("play_text") or "")
    return bool(_DEAD_BALL_PENALTY.match(text)) and "NO PLAY" in text.upper()


# "Timeout Grambling, clock 10:53" / "(02:00) Officials Timeout". Same shape as cbs_check._TIMEOUT so both sides of
# the backup check agree on what counts as a timeout row.
_TIMEOUT_TEXT = re.compile(r"^\s*(\(\d{1,2}:\d{2}\)\s*)?(officials\s+)?time\s*out", re.I)


def is_quarter_issue(entry, changes=None, guess=False):
    """Does this row count toward its quarter's issue share (the coach's 10% backup prompt, the admin board)?
    An issue is a red row, a row the check fixed or added, or a clock still a guess (measured Sep 12 2026 on 125
    games). Roger, Sep 19 2026 (SMU at Louisville, Q1): "an unverified timeout is not really an issue.. just
    information for action" - a row whose ONLY note is the timeout note is not an issue."""
    if entry.get("cbs_tail"):
        return False                     # a CBS tail row (cbs_tail.py) is the data the coach asked for, not an issue
    qc = str(entry.get("qc_issue") or "").strip()
    if qc:
        parts = [p.strip() for p in qc.replace(" \u00b7 ", " | ").split(" | ") if p.strip()]
        if parts and all(p == TIMEOUT_UNVERIFIED_NOTE for p in parts):
            qc = ""
    return bool(qc or changes or entry.get("ncaa_status") == "added" or guess)


def _qc_flag_entries(entries, home_name, away_name):
    """
    Run QC checks on fully mapped + lagged entries.
    Returns {play_index: "short description"} for plays that have issues
    our pipeline could NOT automatically fix.
    Clean plays are absent from the dict (not returned as empty string here;
    caller sets entry["qc_issue"] = flags.get(i, "")).
    """
    flags = {}   # {play_index: [msg, ...]}

    # Score jumps
    for i in range(1, len(entries)):
        hd = entries[i]["home_score"] - entries[i - 1]["home_score"]
        ad = entries[i]["away_score"] - entries[i - 1]["away_score"]
        for delta in (hd, ad):
            if delta == 0 or delta in _QC_BUNDLED_ART:
                continue
            if delta < 0:
                flags.setdefault(i, []).append(f"Score dropped {delta}")
            elif delta not in _QC_VALID_POS:
                flags.setdefault(i, []).append(f"Score jumped +{delta}")

    # Stuck clock: _QC_STUCK_THRESH+ SCRIMMAGE plays on one clock in one quarter. A row that carries the previous
    # play's clock BY NATURE - a try, a kickoff, a timeout, an officials' stoppage, a bookkeeping line, a dead-ball
    # penalty - neither counts toward a run nor breaks one; it rides along inside it.
    # Fresno St @ San Jose St Q2 14:40 (Sep 19 2026): an officials timeout, a false start "NO PLAY" and the snap
    # after them were reported as a 3-play stall. Roger: "it looks right on the tree, it's 3 that should be stuck."
    # The old rule tested only the CURRENT row's down and never the row above it, so a timeout could ANCHOR the very
    # run it should have been left out of.
    # MEASURED, Sep 12 + Sep 19 corpora (249 games, final rows): 159 flags -> 84. All 87 dropped runs were read: every
    # one is a try + kickoff + dead-ball penalty on one clock, stacked timeouts, or overtime (every OT row reads
    # 0:00). No stalled crew lost - the real ones (e.g. RGV @ NICH Q3, five snaps on 0:00) are still flagged.
    # ⚠ cbs_check._stuck_runs stays BROAD on purpose and no longer mirrors this exactly: it decides which rows to
    # re-check against CBS, where over-including costs nothing (CBS agrees -> no change) and it already drops
    # timeouts and admin lines before repairing anything.
    import ncaa_match as _nm
    streak, prev = 0, None
    for i, c in enumerate(entries):
        if _nm._shares_clock_by_nature(c) or _is_dead_ball_penalty(c):
            continue
        if (prev is not None and c.get("clock") == prev.get("clock")
                and c.get("quarter") == prev.get("quarter")):
            streak += 1
        else:
            streak = 1
        if streak == _QC_STUCK_THRESH:
            flags.setdefault(i, []).append(f"Clock stuck ({streak}+ plays)")
        prev = c

    # A row sitting above a play whose clock the backup check CORRECTED, with a lower clock of its own, is provably
    # out of place: the corrected clock is fenced by both sources, this row's is not. Syracuse @ Pitt Q4 (Sep 17-18
    # 2026): ESPN's second kickoff line, stamped 2:07, stayed above five plays put back on 3:08-2:15 - it is ESPN's
    # own duplicate of the onside kick and nothing could vouch for it. The play-order check did not see it (it
    # compares paired plays, and this row has no pair), so the coach saw 3:03 / 2:56 / 2:07 / 3:08 with no flag.
    # Roger: "166-169 are now out of order". Only against a corrected neighbour, so an ordinary game never sees it.
    for i in range(len(entries) - 1):
        c, n = entries[i], entries[i + 1]
        if c.get("quarter") != n.get("quarter") or n.get("ncaa_status") != "fixed":
            continue
        if not any(isinstance(ch, dict) and ch.get("field") == "clock" for ch in (n.get("ncaa_changes") or [])):
            continue
        if (c.get("home_time_out") == "Yes" or c.get("away_time_out") == "Yes"
                or str(c.get("down") or "") == "OTO" or str(c.get("play_text") or "").strip().lower().startswith("timeout")):
            continue
        mine, theirs = _clock_to_seconds(str(c.get("clock") or "")), _clock_to_seconds(str(n.get("clock") or ""))
        if mine and theirs and mine + 5 < theirs:
            flags.setdefault(i, []).append("Out of order - check this play")

    # Missing EP — only fires when _infer_missing_pats also failed
    for i in range(1, len(entries)):
        hd = entries[i]["home_score"] - entries[i - 1]["home_score"]
        ad = entries[i]["away_score"] - entries[i - 1]["away_score"]
        if hd == 6 or ad == 6:
            n1 = str(entries[i].get("down", ""))
            n2 = str(entries[i + 1].get("down", "")) if i + 1 < len(entries) else ""
            if n1 not in ("EP", "2PT") and n2 not in ("EP", "2PT"):
                # If the +6 delta lands on a KO entry the missing EP belongs to
                # the preceding TD — flag that row so the red highlight appears
                # on the TD play, not the kickoff.
                if n1 == "KO" and i > 0:
                    flag_idx = i - 1
                elif str(entries[i - 1].get("down", "")) == "KO" and i >= 2:
                    # +6 appeared right after a KO. If that KO opened a new
                    # period (different quarter than the play before it), the
                    # score jump is from end-of-period plays filtered by the
                    # pipeline — not a missing EP we can reliably detect here.
                    if entries[i - 1].get("quarter", 0) != entries[i - 2].get("quarter", 0):
                        continue
                    flag_idx = i
                else:
                    flag_idx = i
                flags.setdefault(flag_idx, []).append("Missing EP after TD")

    return {idx: " · ".join(msgs) for idx, msgs in flags.items()}


# ============================================================
# Play Parsing
# ============================================================

# ⚠ A DEFENSIVE / SPECIAL-TEAMS TD MUST NEVER BE CREDITED TO THE OFFENSE.
# Sep 4 2026, UTEP at Oklahoma: a punt-return TD briefly showed 7-6 in the live
# tree — the 6 credited to UTEP, who had possession, instead of Oklahoma, who
# returned it. Roger: "SOmetimes the DEfense scores".
#
# The score DELTA is the reliable signal, but it is exactly what is missing at
# the moment these plays arrive: ESPN lags the score update most on return
# touchdowns. With no delta the old code fell through to "whoever had the ball",
# which is the wrong team by definition on a return TD.
#
# ESPN names the play type outright ("Punt Return Touchdown"), so use that as a
# second, lag-proof signal BEFORE falling back to possession.
_RETURN_TD_MARKERS = (
    "return touchdown",      # punt / kickoff / interception / fumble return TD
    "interception return",
    "fumble return",
    "blocked field goal touchdown",
    "blocked punt touchdown",
    "missed field goal return",
)


def _is_return_touchdown(type_text: str) -> bool:
    """True when the play type says the NON-possessing team scored."""
    t = (type_text or "").lower()
    return any(m in t for m in _RETURN_TD_MARKERS)


def _annotate_td_scoring_teams(all_plays):
    """
    Set play["_td_scoring_team"] = "home" or "away" on every play that
    carries PAT data (native or injected by _infer_missing_pats).

    Uses actual score deltas — NOT drive_team_id — so defensive TDs
    (pick-6, fumble return, blocked-kick TD, punt return TD) are
    attributed correctly.  drive_team_id is the OFFENSIVE team that had
    the ball; for a defensive or special-teams TD that is the WRONG team
    to credit with the score.

    ESPN sometimes lags the score update to the NEXT play (especially on
    special-teams scoring plays like punt returns).  If the delta on the
    scoring play itself is < 6 we look ahead up to 2 plays to find
    where the score actually changed.

    Must be called AFTER _infer_missing_pats.
    """
    prev_home = 0
    prev_away = 0
    for i, play in enumerate(all_plays):
        curr_home = play.get("home_score", 0)
        curr_away = play.get("away_score", 0)
        if play.get("point_after_attempt") is not None:
            home_delta = curr_home - prev_home
            away_delta = curr_away - prev_away
            # ESPN sometimes lags the score update to the following play
            # (common on special-teams TDs like punt returns).  Look
            # ahead to find where the score actually jumped.
            if home_delta < 6 and away_delta < 6:
                for look in range(1, 3):
                    if i + look < len(all_plays):
                        fwd = all_plays[i + look]
                        fwd_hd = fwd.get("home_score", 0) - prev_home
                        fwd_ad = fwd.get("away_score", 0) - prev_away
                        if fwd_hd >= 6 or fwd_ad >= 6:
                            home_delta = fwd_hd
                            away_delta = fwd_ad
                            break
            if home_delta >= 6:
                play["_td_scoring_team"] = "home"
            elif away_delta >= 6:
                play["_td_scoring_team"] = "away"
        prev_home = curr_home
        prev_away = curr_away


def _infer_missing_pats(all_plays):
    """
    For TD plays with no embedded PAT data, infer the result from the
    score jump on that play vs the previous play and inject a synthetic
    point_after_attempt so map_espn_play() can generate the EP/2PT row.

    Only injects when there is NO separate EP/2PT play already following
    in the next 2 plays (avoids double-injecting when ESPN reports both).

    Score jumped by 7 = EP good, 8 = 2PT good, 6 = EP missed.
    """
    inferred = []
    prev_home = 0
    prev_away = 0
    for i, play in enumerate(all_plays):
        curr_home = play.get("home_score", 0)
        curr_away = play.get("away_score", 0)
        if play.get("score_value") == 6 and play.get("point_after_attempt") is None:
            # Check if ESPN already has a separate EP/2PT play following
            next_has_pat = False
            for look in range(1, 3):
                if i + look < len(all_plays):
                    nt = all_plays[i + look].get("play_type_text", "").lower()
                    if ("extra point" in nt or "two-point" in nt
                            or "two point" in nt or "pat" in nt):
                        next_has_pat = True
                        break
            if not next_has_pat:
                home_delta = curr_home - prev_home
                away_delta = curr_away - prev_away
                delta = max(home_delta, away_delta)
                if delta == 7:
                    play["point_after_attempt"] = {"text": "Extra Point Good", "value": 1}
                    inferred.append("Inferred missing EP result (+1)")
                elif delta == 8:
                    play["point_after_attempt"] = {"text": "Two-Point Conversion", "value": 2}
                    inferred.append("Inferred missing 2PT result (+2)")
                elif delta == 6:
                    play["point_after_attempt"] = {"text": "Extra Point Attempt - No Good", "value": 0}
                    inferred.append("Inferred missing EP miss (+0)")
        prev_home = curr_home
        prev_away = curr_away
    return inferred


def _parse_play(play, drive_team_id, home_team_id, away_team_id):
    play_id = str(play.get("id", ""))
    if not play_id:
        return None
    play_type = play.get("type", {})
    type_text = play_type.get("text", "")
    type_id = int(play_type.get("id", 0))
    _skip = type_text.lower()

    # Filter pure administrative entries with no football content
    if _skip in ("end period", "end of half", "end of game", "coin toss"):
        return None

    # Detect officials timeout / two-minute warning vs team timeout.
    # Team timeouts (type_text="Timeout" or type_id=21) must NOT be treated as OTO —
    # ESPN sometimes labels them "Official Timeout #1 by Air Force at 2:45" in the
    # description, which would otherwise be caught by the text-based OTO check.
    # Authoritative ESPN play-type ids: 74 = Official Timeout, 75 = Two-minute warning.
    # Both are emitted as OTO rows (yellow tree row + red scoreboard background).
    _text_lower = play.get("text", "").lower()
    # ESPN's college-football feed delivers the two-minute warning as a type-21
    # "Timeout" with an EMPTY team name ("Timeout , clock 02:00") — NOT as type 75.
    # A real team timeout always names the team ("Timeout SMU, clock 08:46"). Treat
    # a blank-team timeout as an officials/administrative stoppage (OTO): it burns no
    # team timeout and turns the scoreboard red.
    _stripped = _text_lower.strip()
    _blank_team_to = (
        (type_id == 21 or _skip == "timeout")
        and _stripped.startswith("timeout")
        and _stripped[len("timeout"):].split(",")[0].strip() == ""
    )
    _is_team_timeout = (_skip == "timeout" or type_id == 21) and not _blank_team_to
    _is_oto = (not _is_team_timeout and
               (type_id in (74, 75) or _blank_team_to or
                "official timeout" in _skip or
                "officials time out" in _skip or
                "two-minute warning" in _skip or
                "two minute warning" in _skip or
                "official timeout" in _text_lower or
                "officials time out" in _text_lower or
                "two-minute warning" in _text_lower or
                "two minute warning" in _text_lower))

    clock_obj = play.get("clock", {})
    clock_display = clock_obj.get("displayValue", "0:00")
    period_num = int(play.get("period", {}).get("number", 1))
    home_score = int(play.get("homeScore", 0))
    away_score = int(play.get("awayScore", 0))
    text = play.get("text", "")
    sequence_number = int(play.get("sequenceNumber", "0"))

    # Officials timeout: emit an OTO row instead of silently skipping
    if _is_oto:
        return {
            "espn_play_id": play_id,
            "sequence_number": sequence_number,
            "period": period_num,
            "clock": clock_display,
            "play_type_text": "Officials Time Out",
            "play_type_id": type_id,
            "description": text or "Officials Timeout",
            "home_score": home_score,
            "away_score": away_score,
            "start_down": None, "start_distance": None, "yards_to_endzone": None,
            "start_yard_line": 0, "start_team_id": "", "stat_yardage": 0,
            "scoring_play": False, "score_value": 0, "drive_team_id": "",
            "end_down": None, "end_distance": None, "end_yards_to_endzone": None,
            "point_after_attempt": None,
            "wallclock": play.get("wallclock", ""),
            "is_officials_timeout": True,
        }

    start = play.get("start", {})
    end = play.get("end", {})
    start_down = start.get("down", None)
    start_distance = start.get("distance", None)
    yards_to_endzone = start.get("yardsToEndzone", None)
    start_yard_line = start.get("yardLine", 0)
    start_team = start.get("team", {})
    start_team_id = str(start_team.get("id", "")) if start_team else ""
    stat_yardage = play.get("statYardage", 0)
    scoring_play = play.get("scoringPlay", False)
    score_value = play.get("scoreValue", 0)
    point_after = play.get("pointAfterAttempt")
    pat_data = None
    if point_after:
        pat_data = {
            "text": point_after.get("text", ""),
            "value": int(point_after.get("value", 0)),
        }
    return {
        "espn_play_id": play_id,
        "sequence_number": sequence_number,
        "period": period_num,
        "clock": clock_display,
        "play_type_text": type_text,
        "play_type_id": type_id,
        "description": text,
        "home_score": home_score,
        "away_score": away_score,
        "start_down": start_down,
        "start_distance": start_distance,
        "yards_to_endzone": yards_to_endzone,
        "start_yard_line": start_yard_line,
        "start_team_id": start_team_id,
        "stat_yardage": stat_yardage,
        "scoring_play": scoring_play,
        "score_value": score_value,
        "drive_team_id": drive_team_id,
        "end_down": end.get("down"),
        "end_distance": end.get("distance"),
        "end_yards_to_endzone": end.get("yardsToEndzone"),
        "point_after_attempt": pat_data,
        "wallclock": play.get("wallclock", ""),
    }

# ============================================================
# Play Mapping (full CAPP format)
# ============================================================

def map_espn_play(play, home_team_id, away_team_id, home_team_display, away_team_display,
                  home_team_abbrev="", away_team_abbrev=""):
    results = []
    type_id = play.get("play_type_id", 0)
    type_text = play.get("play_type_text", "")
    description = play.get("description", "")
    drive_team_id = play.get("drive_team_id", "")
    period = play.get("period", 1)
    quarter = str(period) if period <= 4 else "OT"
    clock = play.get("clock", "0:00")
    home_score = play.get("home_score", 0)
    away_score = play.get("away_score", 0)

    # Officials timeout: emit a single OTO row — no possession/down/gain
    if play.get("is_officials_timeout"):
        return [{
            "home_score":     home_score,
            "away_score":     away_score,
            "clock":          clock,
            "quarter":        quarter,
            "down":           "OTO",
            "distance":       0,
            "gain":           0,
            "field_position": "",
            "possession":     "",
            "run_clock":      "No",
            "home_time_out":  "No",
            "away_time_out":  "No",
            "play_text":      description or "Officials Timeout",
            "wallclock":      play.get("wallclock", ""),
            "espn_play_id":   play.get("espn_play_id", ""),
            "espn_seq":       play.get("sequence_number"),
            "qc_issue":       "",
        }]

    type_text_lower = type_text.lower()

    is_kickoff = "kickoff" in type_text_lower and "return" not in type_text_lower
    if is_kickoff:
        possession = away_team_display if drive_team_id == home_team_id else home_team_display
    elif drive_team_id == home_team_id:
        possession = home_team_display
    elif drive_team_id == away_team_id:
        possession = away_team_display
    else:
        possession = home_team_display

    yards_to_endzone = play.get("yards_to_endzone")
    field_position = convert_field_position(yards_to_endzone)
    start_down = play.get("start_down")
    start_distance = play.get("start_distance", 0)
    stat_yardage = play.get("stat_yardage", 0)
    scoring = play.get("scoring_play", False)

    is_timeout    = type_text_lower == "timeout" or type_id == 21
    is_punt       = "punt" in type_text_lower
    # A turnover ends the offense's play; anything the defence does afterwards is
    # a RETURN, not a gain. ESPN is inconsistent about this - statYardage carries
    # the return yardage on some interceptions and 0 on others - so the column
    # disagreed with itself game to game.
    is_interception = ("interception" in type_text_lower
                       or "intercepted" in description.lower())
    is_defensive_return = ("fumble return" in type_text_lower
                           or "fumble recovery" in type_text_lower)
    is_field_goal = "field goal" in type_text_lower
    is_extra_point = "extra point" in type_text_lower or "pat" in type_text_lower
    is_two_point  = "two-point" in type_text_lower or "two point" in type_text_lower or "2pt" in type_text_lower
    is_rush       = "rush" in type_text_lower and not is_kickoff
    is_pass       = "pass" in type_text_lower or "reception" in type_text_lower
    is_sack       = "sack" in type_text_lower

    if is_kickoff:
        down = "KO"; distance = 0; gain = 0; field_position = -35
    elif is_extra_point:
        down = "EP"; distance = 3; gain = 0; field_position = 3
    elif is_two_point:
        down = "2PT"; distance = 3; gain = 0; field_position = 3
    elif is_punt:
        down = str(start_down) if start_down else "4"
        distance = start_distance if start_distance else 10
        gain = stat_yardage
    elif is_field_goal:
        down = "FG"
        distance = start_distance if start_distance else 10
        gain = stat_yardage
    elif is_timeout:
        down = str(start_down) if start_down else "1"
        distance = start_distance if start_distance else 10
        gain = 0
        # THE SPOT ON A TEAM TIMEOUT IS MEASURED FROM THE WRONG END. Roger, Maryland Q2 0:01 (Sep 19 2026): "the
        # down and distance doesn't make sense coming out of the timeouts ... that way someone doesn't see them and
        # think they are wonky." Maryland's drive read -37, then the timeout row +37, then -42.
        # Read in ESPN's raw play: on a team timeout `start.team` is the team that CALLED it, not the team with the
        # ball, and `yardsToEndzone` is measured to THAT team's end zone. When the defence calls it the two are
        # opposite ends, so the spot lands on the far side of the field. Measured on the raw feed: start.team ==
        # drive team on 334 timeouts and DIFFERENT on 143 (Sep 19, first 40 games) - the 143 are the flipped ones.
        # Mirroring the yard line back onto the offense's half is all that is needed: down and distance already
        # carry the previous snap's numbers and are left alone.
        # ⚠ NOT done by copying the row above: ~50 timeouts per corpus sit after a kickoff, a try or a change of
        # possession, where the snap above belongs to the OTHER team (Tennessee @ Georgia Tech Q4 1:38 would have
        # taken Georgia Tech's 1&2 at the 2 onto a Tennessee timeout).
        _to_team_id = str(play.get("start_team_id") or "")
        if (_to_team_id and drive_team_id and _to_team_id != str(drive_team_id)
                and yards_to_endzone is not None):
            field_position = convert_field_position(100 - yards_to_endzone)
    else:
        down = str(start_down) if start_down else "1"
        distance = start_distance if start_distance else 10
        gain = stat_yardage

    # GAIN IS AN OFFENSIVE NUMBER. On a turnover the offense gained nothing, no
    # matter how far the ball is carried back. Applied after the branches above
    # so it covers whichever one the play fell into.
    # NOTE: a rush or completion that ends in a fumble keeps its yardage - those
    # yards were really gained before the ball was lost - so only interceptions
    # and plays ESPN itself types as a defensive return are zeroed here.
    if is_interception or is_defensive_return:
        gain = 0

    run_clock = "No"
    if is_rush and not scoring and not is_kickoff:
        end_down = play.get("end_down")
        if end_down != 1 and gain < distance:
            run_clock = "Yes"

    home_time_out = away_time_out = "No"
    if is_timeout:
        desc_lower = description.lower()
        # Accent-folded copy: CAPP names are ASCII, ESPN play text is not.
        desc_folded = _fold_accents(desc_lower)
        home_abbrevs = _timeout_abbrevs(home_team_abbrev)
        away_abbrevs = _timeout_abbrevs(away_team_abbrev)
        if _abbrev_in_text(home_abbrevs, desc_lower):
            home_time_out = "Yes"
        elif _abbrev_in_text(away_abbrevs, desc_lower):
            away_time_out = "Yes"
        elif home_team_display and _fold_accents(home_team_display.lower()) in desc_folded:
            home_time_out = "Yes"
        elif away_team_display and _fold_accents(away_team_display.lower()) in desc_folded:
            away_time_out = "Yes"
        elif "home" in desc_lower:
            home_time_out = "Yes"
        elif "away" in desc_lower or "visitor" in desc_lower:
            away_time_out = "Yes"

    pat = play.get("point_after_attempt")
    td_home_score = home_score
    td_away_score = away_score
    if pat and not is_kickoff and not is_field_goal and not is_extra_point and not is_two_point:
        pat_value = pat.get("value", 0)
        if pat_value > 0:
            # Use score-delta annotation when available — drive_team_id is the
            # OFFENSIVE team and is wrong for defensive / special-teams TDs
            # (pick-6, fumble return, blocked-kick TD, punt return TD).
            td_scorer = play.get("_td_scoring_team")
            if td_scorer:
                scored_home = (td_scorer == "home")
            elif _is_return_touchdown(type_text_lower):
                # ESPN's own play type says the receiving/defending team scored,
                # so credit the team that did NOT have the ball. This works even
                # when the score has not updated yet, which is when it matters.
                scored_home = (drive_team_id != home_team_id)
            elif is_punt:
                # Punt plays: drive_team_id is the PUNTING team.
                # A scoring punt must be a punt return TD — the RECEIVING
                # team scored, which is the opposite of the drive team.
                scored_home = (drive_team_id != home_team_id)
            else:
                scored_home = (drive_team_id == home_team_id)
            if scored_home:
                td_home_score = home_score - pat_value
            else:
                td_away_score = away_score - pat_value

    entry = {
        "home_score": td_home_score,
        "away_score": td_away_score,
        "clock": clock,
        "quarter": quarter,
        "down": down,
        "distance": distance,
        "gain": gain,
        "field_position": field_position,
        "possession": possession,
        "run_clock": run_clock,
        "home_time_out": home_time_out,
        "away_time_out": away_time_out,
        "play_text": description,
        "wallclock": play.get("wallclock", ""),
        "espn_play_id": play.get("espn_play_id", ""),
        # ESPN's slot for this play. A crew-inserted play shares its predecessor's number, so order is
        # (espn_seq, espn_play_id). Clients save it with the play so a reopened game keeps its order.
        "espn_seq": play.get("sequence_number"),
    }
    results.append(entry)

    if pat and not is_kickoff and not is_field_goal and not is_extra_point and not is_two_point:
        pat_text = pat.get("text", "").lower()
        pat_value = pat.get("value", 0)
        is_two_point_pat = "two" in pat_text or "2pt" in pat_text or "2-point" in pat_text or pat_value == 2
        # ⚠ A FAILED RUSH two-point try comes from ESPN as pointAfterAttempt "Not Available", value 0 - nothing says
        # "two", so it became an EP row (Sep 12: 6 of 125 games, e.g. Buffalo @ FIU "... TOUCHDOWN, clock 05:58, 1ST
        # DOWN #7 J.Wright rush attempt failed"). The touchdown's own text names the try; a kick never reads
        # "rush/pass attempt". Roger, Sep 14 2026: "Fix the failed rush two-point tries to be 2PT We need That as a play".
        if not is_two_point_pat and _TWO_POINT_TRY_TEXT.search(str(description or "")):
            is_two_point_pat = True
        # The try happens AFTER the touchdown ends, so it takes the TD's END time, which the crew writes
        # into the TD line ("... TOUCHDOWN, clock 00:24"). The TD row itself now carries its SNAP time
        # (apply_text_snap_clocks), which would put the PAT 5 seconds early (SMU Q2 replay, Sep 13 2026).
        _end = re.search(r"clock\s+(\d{1,2}):(\d{2})", str(description or ""), re.I)
        pat_clock = f"{int(_end.group(1))}:{_end.group(2)}" if _end else clock
        pat_entry = {
            "home_score": home_score,
            "away_score": away_score,
            "clock": pat_clock,
            "quarter": quarter,
            "down": "2PT" if is_two_point_pat else "EP",
            "distance": 3,
            "gain": 0,
            "field_position": 3,
            "possession": possession,
            "run_clock": "No",
            "home_time_out": "No",
            "away_time_out": "No",
            "play_text": pat.get("text", ""),
            "wallclock": play.get("wallclock", ""),
            "espn_play_id": play.get("espn_play_id", ""),
            "espn_seq": play.get("sequence_number"),
        }
        results.append(pat_entry)

    return results

# ============================================================
# Scoreboard Fetching
# ============================================================

def _week_to_date_range(season, week, seasontype=None):
    if seasontype == 3:
        dates = _POSTSEASON_DATES.get(season)
        if dates:
            return f"{dates[0]}-{dates[1]}"
        return None
    season_weeks = _SEASON_WEEK_DATES.get(season)
    if not season_weeks:
        return None
    dates = season_weeks.get(week)
    if not dates:
        return None
    return f"{dates[0]}-{dates[1]}"

def _fetch_scoreboard(league, params):
    url = NFL_SCOREBOARD_URL if league == "nfl" else CFB_SCOREBOARD_URL
    try:
        r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json().get("events", [])
    except Exception as e:
        print(f"Scoreboard error ({league}): {e}")
        return []

def _events_to_games(events, league):
    games = []
    for event in events:
        competition = event.get("competitions", [{}])[0]
        status_obj = competition.get("status", {})
        state = status_obj.get("type", {}).get("state", "pre")
        detail = status_obj.get("type", {}).get("shortDetail", "")
        clock = status_obj.get("displayClock", "0:00")
        period = status_obj.get("period", 0)

        home = away = None
        for competitor in competition.get("competitors", []):
            score_raw = competitor.get("score", 0)
            score = int(score_raw.get("value", 0) if isinstance(score_raw, dict) else score_raw or 0)
            info = {
                "team": competitor.get("team", {}).get("displayName", ""),
                "abbrev": competitor.get("team", {}).get("abbreviation", ""),
                "score": score,
                "team_id": competitor.get("team", {}).get("id", ""),
            }
            if competitor.get("homeAway") == "home":
                home = info
            else:
                away = info

        if not home or not away:
            continue

        conf_name = ""
        if competition.get("conferenceCompetition", False):
            grp = competition.get("groups") or {}
            conf_name = grp.get("shortName") or grp.get("name", "")

        games.append({
            "game_id": event.get("id", ""),
            "league": league,
            "home_team": home["team"],
            "home_abbrev": home["abbrev"],
            "home_score": home["score"],
            "home_team_id": home["team_id"],
            "away_team": away["team"],
            "away_abbrev": away["abbrev"],
            "away_score": away["score"],
            "away_team_id": away["team_id"],
            "status": state,
            "status_detail": detail,
            "period": period,
            "clock": clock,
            "conference": conf_name,
            "date": event.get("date", ""),
        })
    return games

# In-memory cache for week/date-range scoreboard lookups (year+week given to
# get_live_games). This was a raw, uncached ESPN pass-through — every client
# request re-hit ESPN directly, defeating the whole point of this server
# existing as a shared cache layer (surfaced Jul 20 2026 when a client-side
# fix started calling this 16x per "load full schedule" click). 5 min TTL:
# short enough that gameday status/scores still refresh promptly for anyone
# actively browsing, long enough to collapse repeated hits across users
# within the same window into one real ESPN call.
_historical_games_cache: dict = {}
_historical_games_ts: dict = {}
_HISTORICAL_GAMES_CACHE_SECONDS = 300

def _fetch_historical_games(league, year, week, seasontype=2):
    cache_key = f"{league}_{year}_{week}_{seasontype}"
    now = time.time()
    if cache_key in _historical_games_cache and \
            now - _historical_games_ts.get(cache_key, 0) < _HISTORICAL_GAMES_CACHE_SECONDS:
        return _historical_games_cache[cache_key]

    results = []
    leagues = ["cfb", "nfl"] if league == "all" else [league]
    for lg in leagues:
        if lg == "nfl":
            params = {"year": year, "week": week, "seasontype": seasontype}
        else:
            date_range = _week_to_date_range(year, week, seasontype if seasontype == 3 else None)
            params = {"dates": date_range} if date_range else {}
        events = _fetch_scoreboard(lg, params)
        results.extend(_events_to_games(events, lg))

    _historical_games_cache[cache_key] = results
    _historical_games_ts[cache_key] = now
    return results

# ============================================================
# Play Fetching + Full Mapping Pipeline
# ============================================================

TIMEOUTS_FROM_CBS = True          # CBS's timeouts-remaining counters decide (measured Sep 19 2026 - see _classify_timeouts)
# CBS shows the lower count ON THE PLAY the timeout was called after, so the drop's snap clock reads a few seconds to a
# minute EARLIER than ESPN's timeout clock (MEASURED Sep 19 2026 on the Sep 12 corpus: the first version allowed the
# drop to be later and fell from 98% to 84% agreement with NCAA's labels).
_CBS_TO_BEFORE = 60               # the drop's snap can be up to this many game seconds before the timeout's clock
_CBS_TO_AFTER = 30                # ... or up to half a minute after it, when one side's clock is typed off
_CBS_TO_OTHER_TEAM = 100          # a drop for the OTHER team than ESPN names: only when nothing better fits
_CBS_TO_UNNAMED = 25              # an ESPN row with no team (an "Officials Timeout"): a named row wins a near tie


def _cbs_timeout_drops(cbs_doc):
    """CBS's charged timeouts from its timeouts-remaining counters (cbs_backup.team_timeouts), or None when CBS has no
    counters for this game. Each: quarter, clock_secs of the play the counter dropped on, side."""
    if not cbs_doc or not cbs_doc.get("available") or not cbs_doc.get("teams_known") or not cbs_doc.get("counters_known"):
        return None
    out = []
    for it in cbs_doc.get("items") or []:
        if it.get("kind") == "team_timeout" and it.get("timeout_side") in ("home", "away") and it.get("clock_secs") is not None:
            try:
                q = int(str(it.get("quarter")).strip())
            except (TypeError, ValueError):
                continue
            out.append({"q": q, "secs": int(it["clock_secs"]), "side": it["timeout_side"]})
    return out


def _fold_team(name):
    return re.sub(r"[^a-z0-9 ]+", "", str(name or "").lower()).strip()


def _cbs_drop_score(d, q, secs, side):
    """How well a counter drop fits an ESPN timeout row (lower is better), or None when it cannot be that timeout.
    A row ESPN typed as an officials' timeout (no team) is only charged by a drop sitting right on it (10 s)."""
    # MEASURED Sep 19 2026 (Sep 12 corpus, 69 games, 682 timeout rows): charging a row for the OTHER team than it names,
    # or charging a row the crew typed "Officials Timeout", both lost against NCAA's labels (a duplicate "Timeout
    # Kentucky" row took Alabama's drop; a TV break on the same second as a counter drop got charged). So CBS only ever
    # CONFIRMS the team ESPN names: a named row with a same-team drop in the window is charged, a named row with none
    # is an officials' timeout unless NCAA charges it, and an unnamed row is left to the older rules.
    if q is None or secs is None or d["q"] != q or side not in ("home", "away") or side != d["side"]:
        return None
    lead = d["secs"] - secs                      # positive = the drop's play sits earlier on the clock than the timeout
    if not (-_CBS_TO_AFTER <= lead <= _CBS_TO_BEFORE):
        return None
    return abs(lead)


def _assign_cbs_drops(rows, drops):
    """{row index: drop} - BEST PAIR FIRST over the whole game, each row and each drop used once. (Row order was tried
    first and was wrong: an earlier row stole a later row's drop - Arkansas @ Utah Q4, the 2:00 row took 1:45's.)"""
    pairs = []
    for i, q, secs, side in rows:
        for k, d in enumerate(drops):
            sc = _cbs_drop_score(d, q, secs, side)
            if sc is not None:
                pairs.append((sc, i, k))
    out, used = {}, set()
    for sc, i, k in sorted(pairs):
        if i in out or k in used:
            continue
        out[i] = drops[k]
        used.add(k)
    return out


# Sep 19 2026 (Roger, Maryland vs Virginia Tech live): ESPN's crew typed 13 team timeouts in a half - three for
# Virginia Tech in Q1 alone - while CBS's timeouts-remaining counters dropped exactly 3 times. "No one is going to
# call a timeout before a kickoff after a scoring play." So when CBS has counters for the game they decide FIRST:
# an ESPN timeout row that lines up with a counter drop is charged to that team; one that lines up with nothing is
# an officials' timeout (no charge, OTO). NCAA's labels and the measured fallback rules stay for games CBS does not
# have. MEASURED before it went in: _dev_tools/game_replay_test/measure_timeouts_cbs.py.
def _classify_timeouts(entries, home_name, away_name, league, game_date="", cbs_doc=None):
    """Rewrite timeout rows to say who they are actually charged to.

    Returns (changed_count, examples, unresolved_count). Never raises: a second
    source must not be able to break play delivery.
    """
    if league != "cfb":
        return 0, [], []           # the backup source has no NFL data

    def _is_timeout(e):
        # A ROW THE FEED NEVER FLAGGED IS STILL A TIMEOUT. Grambling @ TCU and Seton Hall @ UMass, Sep 12 2026: rows
        # reading "Timeout Grambling, clock 10:53" arrived with home_time_out/away_time_out both "No" (the team-name
        # match in map_espn_play found neither side) and a normal down, so they were never OTO either - and this
        # function dropped them before any rule could look at them. The coach saw a "?" and had to classify by hand.
        # The body below already knows what to do with an unflagged row: it reads the team out of the text. Same
        # shape cbs_check._TIMEOUT has always used, so both sides of the check agree on what a timeout row is.
        return (str(e.get("home_time_out")) == "Yes"
                or str(e.get("away_time_out")) == "Yes"
                or str(e.get("down", "")).strip().upper() == "OTO"
                or bool(_TIMEOUT_TEXT.match(str(e.get("play_text") or ""))))

    rows = [(i, e) for i, e in enumerate(entries) if _is_timeout(e)]
    if not rows:
        return 0, [], []

    labels = {}
    try:
        import ncaa_live
        found = ncaa_live.resolve_game(
            _season_guess(game_date), home_name, away_name,
            date=_ncaa_date(game_date) or None)
        if found.get("available"):
            for t in ncaa_live.timeouts(found["ncaa_game_id"]).get("timeouts", []):
                labels[(str(t.get("quarter")), ncaa_live._norm_clock(t.get("clock")))] = t
    except Exception as e:
        print(f"WARNING: timeout classify: backup lookup failed "
              f"({home_name} v {away_name}): {e}", flush=True)

    changed, examples, unresolved = 0, [], []
    drops = _cbs_timeout_drops(cbs_doc) if TIMEOUTS_FROM_CBS else None
    assigned = {}
    if drops is not None:
        _rq = []
        for _i, _e in rows:
            try:
                _q = int(str(_e.get('quarter', '')).strip() or 0)
            except (TypeError, ValueError):
                _q = None
            # the clock in the row's own text beats the clock field (the field is often the previous play's)
            _m = re.search(r'clock\s+(\d{1,2}):(\d{2})', str(_e.get('play_text', '')), re.I)
            if not _m:
                _m = re.match(r'^\s*(\d{1,2}):(\d{2})\s*$', str(_e.get('clock', '')))
            _side = 'home' if str(_e.get('home_time_out')) == 'Yes' else ('away' if str(_e.get('away_time_out')) == 'Yes' else None)
            if _side is None:                     # no flag: the team the text names (loose: 'Arkansas' ~ 'Arkansas Razorbacks')
                _tm = re.match(r'^\s*timeout\s+(.+?)\s*(,|$)', str(_e.get('play_text', '')), re.I)
                _who = _fold_team(_tm.group(1)) if _tm else ''
                _h, _a = _fold_team(home_name), _fold_team(away_name)
                if _who and (_who == _h or _who in _h or _h in _who) and not (_who == _a or _who in _a or _a in _who):
                    _side = 'home'
                elif _who and (_who == _a or _who in _a or _a in _who):
                    _side = 'away'
            _rq.append((_i, _q, int(_m.group(1)) * 60 + int(_m.group(2)) if _m else None, _side))
        assigned = _assign_cbs_drops(_rq, drops)
        named = {_i for _i, _q, _s, _side in _rq if _side in ('home', 'away')}
    for i, e in rows:
        key = (str(e.get("quarter")), _norm_clock_str(e.get("clock")))
        lab = labels.get(key)
        if lab is None:
            # the clock written into the play text beats the clock FIELD when
            # they disagree - measured on real rows where ESPN's field was wrong
            m = re.search(r"clock\s+(\d{1,2}:\d{2})", str(e.get("play_text", "")), re.I)
            if m:
                lab = labels.get((str(e.get("quarter")), _norm_clock_str(m.group(1))))

        verdict = why = None
        if lab is not None:
            ch = lab.get("charged")
            if ch in ("home", "away"):
                verdict, why = ch, "backup source"
            elif ch is None:
                verdict, why = "officials", "backup source"
        # MEASURED Sep 19 2026 (Sep 12 corpus, 69 CBS games, 682 timeout rows, 598 with an NCAA label): CBS's counters
        # AHEAD of NCAA's label fell from 98.2% to 95.7% agreement with those labels (the two-minute TV break on the
        # same second as a real drop, a duplicate typed row). So: NCAA's label first, CBS's counters where NCAA is
        # silent (all 13 of Maryland's rows on Sep 19 - NCAA had no labels live), the fallback rules last.
        if verdict is None and drops is not None and i in named:
            _hit = assigned.get(i)
            if _hit is not None:
                verdict, why = _hit['side'], 'backup counters'
            else:
                verdict, why = 'officials', 'backup counters (no timeout charged)'
        if verdict is None:
            ok, reason = _assume_officials(entries, i)
            if ok:
                verdict, why = "officials", reason
        if verdict is None:
            unresolved.append(i)
            continue

        # TWO OFFICIALS' STOPPAGES CANNOT SHARE A TICK. If an OTO row already sits at this quarter and clock, a row
        # the crew typed with a TEAM NAME is something else - and calling it officials would rewrite it into a second
        # identical row, and a second identical board, in front of the coach. Lincoln (PA) vs Mississippi Valley St
        # Q2 2:00 (Sep 12 2026), a game with NO backup source at all, where the fallback did exactly that. Leave it
        # unresolved instead: the coach gets the "check who it is charged to" note and the Classify Timeouts button.
        if verdict == "officials" and _names_a_team(e) and _officials_already_at(entries, i):
            unresolved.append(i)
            continue

        before = (e.get("down"), e.get("home_time_out"), e.get("away_time_out"),
                  e.get("play_text"))
        if verdict == "officials":
            e["down"] = "OTO"
            e["home_time_out"] = e["away_time_out"] = "No"
        else:
            if str(e.get("down", "")).strip().upper() == "OTO":
                e["down"] = ""
            e["home_time_out"] = "Yes" if verdict == "home" else "No"
            e["away_time_out"] = "Yes" if verdict == "away" else "No"
        # The text has to agree with the columns. A row reading "Timeout BYU"
        # beside No/No looks broken to a coach, and play_text is a renderable
        # scoreboard element, so the wrong wording can reach a board too.
        e["play_text"] = _timeout_line(
            verdict, home_name if verdict == "home" else away_name,
            e.get("clock"), e.get("play_text"))
        if (e.get("down"), e.get("home_time_out"), e.get("away_time_out"),
                e.get("play_text")) != before:
            changed += 1
            if len(examples) < 5:
                examples.append("Q%s %s timeout -> %s (%s)"
                                % (e.get("quarter"), e.get("clock"), verdict, why))
    return changed, examples, unresolved


def _names_a_team(entry):
    """True when the row reads "Timeout <someone>" rather than a bare/officials stoppage."""
    return bool(re.match(r"^\s*timeout\s+\S", str(entry.get("play_text") or ""), re.I))


def _officials_already_at(entries, i):
    """True when another row in the same quarter already holds an officials' stoppage at this row's clock."""
    e = entries[i]
    q, c = str(e.get("quarter")), _norm_clock_str(e.get("clock"))
    for j, o in enumerate(entries):
        if j != i and str(o.get("quarter")) == q \
                and str(o.get("down", "")).strip().upper() == "OTO" \
                and _norm_clock_str(o.get("clock")) == c:
            return True
    return False


def _timeout_line(verdict, team_name, clock, existing):
    """What a timeout row should read once classified.

    Only rewrites a line that ALREADY reads as a timeout, so an unrelated play
    can never be overwritten, and keeps the feed's own shape
    "Timeout <who>, clock MM:SS" so nothing downstream sees a new format.
    """
    existing = str(existing or "")
    if not re.match(r"^\s*(officials\s+)?timeout\b", existing, re.I):
        return existing
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(clock or ""))
    tail = ", clock %02d:%s" % (int(m.group(1)), m.group(2)) if m else ""
    if verdict == "officials":
        return "Officials Timeout" + tail
    return "Timeout %s%s" % (team_name, tail)


def _norm_clock_str(c):
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(c or ""))
    return "%d:%s" % (int(m.group(1)), m.group(2)) if m else str(c or "").strip()


def _ncaa_date(game_date):
    """ESPN's ISO stamp -> MM/DD/YYYY. The resolver retries without it anyway."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(game_date or ""))
    return "%s/%s/%s" % (m.group(2), m.group(3), m.group(1)) if m else ""


def _season_guess(game_date):
    m = re.match(r"(\d{4})", str(game_date or ""))
    if m:
        # January bowls belong to the previous season
        y = int(m.group(1))
        mm = re.match(r"\d{4}-(\d{2})", str(game_date))
        return y - 1 if mm and mm.group(1) in ("01", "02") else y
    import datetime as _dt
    return _dt.date.today().year


_TO_SCORING = re.compile(r"touchdown|field goal.*good|kick attempt good|extra point|two[ -]?point", re.I)
_TO_KICKOFF = re.compile(r"\bkickoff\b|\bkicks off\b|\bkicks\b", re.I)


def _assume_officials(entries, index):
    """The measured fallback rules. (True, reason) or (False, "").

    Validated against the backup source's own labels on 201 timeouts across 23
    games: clock exactly 2:00 -> 26 of 27 right; after a score or kick, outside
    OT and the last 2:00 of Q4 -> 28 of 29. "Start of quarter (15:00)" was tried
    and REJECTED: it fired twice and was wrong both times.
    """
    e = entries[index]
    try:
        q = int(str(e.get("quarter", "0")).strip() or 0)
    except (TypeError, ValueError):
        q = 0
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", str(e.get("clock", "")))
    cs = int(m.group(1)) * 60 + int(m.group(2)) if m else None

    if _norm_clock_str(e.get("clock")) == "2:00":
        return True, "two-minute"

    # A trailing team stops the clock right after a score late on - so this rule
    # must not apply in the last two minutes or in overtime. That exclusion is
    # what took it from 87% to 97%.
    if q >= 5 or (q == 4 and cs is not None and cs <= 120):
        return False, ""

    for j in range(index - 1, -1, -1):
        txt = str(entries[j].get("play_text", ""))
        if txt.strip().lower().startswith(("timeout", "officials timeout")):
            continue
        if _TO_SCORING.search(txt):
            return True, "after a score"
        if _TO_KICKOFF.search(txt):
            return True, "after a kick"
        return False, ""
    return False, ""


def _assign_entry_keys(entries):
    """Give every entry an `entry_key` that stays the same from poll to poll.

    ⚠ WHY. The live client used to take new plays by COUNT (`entries[already:]`),
    which assumes the stat crew only ever adds plays at the end. They don't - on
    SMU vs UC Davis (Sep 12 2026) they slotted 11 plays in behind plays already
    published. Each time, the client never showed the inserted play and drew the
    old last play a second time. A key lets the client see exactly which plays
    are new and where they belong.

    Key = the ESPN play id. A PAT row shares its TD's id, so the second row from
    one play is "<id>:2". A score-gap placeholder has no ESPN play, so it is
    anchored to the next real play ("gap:<id>") and survives as long as that does.
    """
    seen = {}
    for i, entry in enumerate(entries):
        base = str(entry.get("espn_play_id") or entry.get("_forced_key") or "")
        if not base:
            nxt = next((str(e["espn_play_id"]) for e in entries[i + 1:]
                        if e.get("espn_play_id")), "end")
            base = f"gap:{nxt}"
        n = seen.get(base, 0) + 1
        seen[base] = n
        entry["entry_key"] = base if n == 1 else f"{base}:{n}"


def _fetch_game_plays_mapped(game_id, league="cfb", summary=None):
    """summary: an ESPN game summary to map instead of fetching one (get_replay_step passes a cut-down copy)."""
    url = NFL_SUMMARY_URL if league == "nfl" else CFB_SUMMARY_URL
    if summary is None:
        r = _session.get(url, params={"event": game_id}, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    else:
        data = summary

    home_team_id = away_team_id = None
    home_team_name = away_team_name = ""
    home_team_abbrev = away_team_abbrev = ""

    header = data.get("header", {})
    for comp in header.get("competitions", [{}]):
        for competitor in comp.get("competitors", []):
            team = competitor.get("team", {})
            tid = str(team.get("id", ""))
            tname = team.get("displayName", "")
            tabbrev = team.get("abbreviation", "")
            if competitor.get("homeAway") == "home":
                home_team_id = tid
                home_team_name = tname
                home_team_abbrev = tabbrev
            else:
                away_team_id = tid
                away_team_name = tname
                away_team_abbrev = tabbrev

    game_status = "in"
    game_detail = ""                                   # ESPN's words: "Halftime", "End of 1st", "Delayed", "7:12 - 2nd"
    for comp in header.get("competitions", [{}]):
        game_status = comp.get("status", {}).get("type", {}).get("state", "in")
        _t = comp.get("status", {}).get("type", {}) or {}
        game_detail = str(_t.get("shortDetail") or _t.get("detail") or "")

    # Get CAPP canonical names for possession field
    capp_home = espn_name_to_capp_name(home_team_name, league) or home_team_name
    capp_away = espn_name_to_capp_name(away_team_name, league) or away_team_name

    # Parse all plays
    all_plays = []
    seen_ids = set()
    drives_data = data.get("drives", {})

    for drive in drives_data.get("previous", []):
        drive_team_id = str(drive.get("team", {}).get("id", ""))
        for play in drive.get("plays", []):
            parsed = _parse_play(play, drive_team_id, home_team_id, away_team_id)
            if parsed and parsed["espn_play_id"] not in seen_ids:
                seen_ids.add(parsed["espn_play_id"])
                all_plays.append(parsed)

    current_drive = drives_data.get("current", {})
    if current_drive:
        drive_team_id = str(current_drive.get("team", {}).get("id", ""))
        for play in current_drive.get("plays", []):
            parsed = _parse_play(play, drive_team_id, home_team_id, away_team_id)
            if parsed and parsed["espn_play_id"] not in seen_ids:
                seen_ids.add(parsed["espn_play_id"])
                all_plays.append(parsed)

    all_plays.sort(key=lambda p: p.get("sequence_number", 0))

    # Infer missing PAT data from score jumps
    inferred_pat_fixes = _infer_missing_pats(all_plays)
    # Annotate which team scored each TD (uses score deltas, not drive_team_id)
    _annotate_td_scoring_teams(all_plays)

    # Fix clocks, estimate snap times. ESPN's own clock is kept first - it outranks our guesses
    # (see apply_text_snap_clocks).
    # A timeout typed in late goes back where its typed time belongs, before any clock work (see reslot_late_timeouts).
    reslot_late_timeouts(all_plays)
    for _p in all_plays:
        _p["_espn_clock"] = _p.get("clock")
    fix_clock_anomalies(all_plays)
    estimate_snap_clocks(all_plays)
    # The snap time typed into the play text wins over both - see apply_text_snap_clocks.
    apply_text_snap_clocks(all_plays)
    clock_src = {str(p.get("espn_play_id")): p.get("_clock_src") for p in all_plays}

    # Map to CAPP format
    entries = []
    for play in all_plays:
        mapped = map_espn_play(
            play, home_team_id, away_team_id,
            capp_home, capp_away,
            home_team_abbrev, away_team_abbrev
        )
        entries.extend(mapped)

    fill_missing_field_positions(entries)
    actual_home, actual_away = apply_scoreboard_lag(entries)

    # Post-lag auto-fix: correct errors that survived the pre-mapping pipeline
    entry_fixes = _auto_fix_entries(entries)

    # ESPN's own scoring summary is the authority on the score: lift any row still carrying a score the summary says
    # was already on the board (a half-ending field goal used to leave the whole next period 3 points short). Runs
    # BEFORE the gap check on purpose - with the scores right, a boundary that only looked like a missing play stops
    # looking like one.
    _events = score_events(data)
    for _i, _msg in _raise_lagging_scores(entries, _events).items():
        entry_fixes.setdefault(_i, _msg)
    # (_lower_spiked_scores, the raise's counterpart, runs later - after the CBS clock fixes and the timeout re-slot,
    # so it sees the rows in their best order: a timeout ESPN filed at the wrong spot carries the score of ITS clock.)

    # Insert placeholder entries for scoring plays missing from the feed
    # (e.g., last-second Q2 TDs filtered as "End Period" type plays)
    inserted_gap_count = _fill_scoring_gaps(entries, capp_home, capp_away, events=_events)

    # The live check (Roger, Sep 14 2026): "ESPN = Primary CBS = Primary Backup for Comparison NCAA = Secondary Backup
    # for ESPN Bad Plays and CBS not Posting Play By Play". ESPN is the play data. When CBS has play-by-play for the
    # game, CBS checks it (cbs_check.py); otherwise NCAA does, exactly as before (ncaa_check.py). NCAA's copy is fetched
    # either way: the server keeps it for the PRIMARY BACKUP button, and it is CBS's check's next clock source.
    # The summary keeps the key "ncaa_check" - every client and test reads that key; "source" says which one ran.
    ncaa_summary = {"available": False, "review_count": 0}
    if league in ("cfb", "nfl"):
        _gd = ""
        for _c in (data.get("header", {}) or {}).get("competitions", [{}]):
            _gd = _c.get("date", "") or _gd
        _pbp, _found = None, {}
        # NFL gets CBS and nothing else (Sep 16 2026). NCAA is college-only by definition (ncaa_live is division=
        # "fbs"), so an NFL game has no second vote and no primary backup. Consequence to know: cbs_check skips its
        # two-source down/distance/spot step without ncaa_pbp (cbs_check.py:489), so NFL gets verification, CBS clock
        # fixes and missing-play adds - not D&D corrections. Timeouts are unchanged too (_classify_timeouts is cfb
        # only, and CBS writes no timeout plays); NFL timeout attribution stays the gamebook-abbreviation path.
        if league == "cfb":
            try:
                import ncaa_live
                _found = ncaa_live.resolve_game(_season_guess(_gd), capp_home, capp_away,
                                                date=_ncaa_date(_gd) or None)
                if _found.get("available"):
                    _pbp = ncaa_live.play_by_play(_found["ncaa_game_id"])
                    if _pbp.get("available") and summary is None:
                        # Primary Backup (Roger, Sep 14 2026): keep NCAA's copy while the game is played - NCAA
                        # rewrites a game later and drops the team from every spot. Never from a replay (summary
                        # given). Never raises, stores on its own thread. See primary_backup.py.
                        import primary_backup
                        primary_backup.note_pbp(game_id, _found["ncaa_game_id"], _pbp)
            except Exception as e:
                print(f"WARNING: NCAA fetch failed for {game_id}: {type(e).__name__}: {e}", flush=True)
        _pbp_ok = bool(_pbp and _pbp.get("available"))
        cbs_ran = False
        try:
            import cbs_live
            import cbs_check
            _cg = cbs_live.find_game(_gd, home_team_id, away_team_id, league)
            if _cg.get("available"):
                _cp = cbs_live.play_by_play(_cg["cbs_game_id"])
                if _cp.get("available"):
                    cbs_ran = True                      # CBS is this game's check even if a replay step has no CBS play yet
                    _cplays = _cp["plays"]
                    if summary is not None:
                        # A replay (Simulate) is a finished game cut back to a step: give CBS as it stood then, never
                        # the finished game (that is what made Simulate's NCAA check add plays early).
                        _cut = max((str(e.get("wallclock") or "") for e in entries), default="")
                        _cplays = [p for p in _cplays if _cut and str(p.get("real_clock") or "") <= _cut]
                    _items = cbs_live.to_items(_cplays, _cg["home_code"], _cg["away_code"], _cg["swapped"])
                    ncaa_summary = cbs_check.verify_entries(entries, _items, capp_home, capp_away, clock_src=clock_src,
                                                            ncaa_pbp=_pbp if _pbp_ok else None,
                                                            cbs_game_id=_cg["cbs_game_id"])
        except Exception as e:
            print(f"WARNING: CBS check failed for {game_id}: {type(e).__name__}: {e}", flush=True)
        if not cbs_ran and _pbp_ok:
            try:
                import ncaa_check
                ncaa_summary = ncaa_check.verify_entries(entries, _pbp, capp_home, capp_away, clock_src=clock_src)
                ncaa_summary["ncaa_game_id"] = _found["ncaa_game_id"]
                ncaa_summary["source"] = "ncaa"
            except Exception as e:
                print(f"WARNING: NCAA check failed for {game_id}: {type(e).__name__}: {e}", flush=True)

    # Timeouts once more, against the CORRECTED clocks (see reslot_timeouts_by_clock). Before the keys and before the
    # timeout classify, so every index those two hand on is still the row it names.
    try:
        _to_moves = reslot_timeouts_by_clock(entries)
    except Exception as e:
        _to_moves = []
        print(f"WARNING: timeout re-slot failed for {game_id}: {type(e).__name__}: {e}", flush=True)

    # Bring down a run of rows carrying a score the game had not reached yet (a play re-typed late took the score of
    # that moment, and the scoreboard lag handed it to the rows after it - SMU Q2, Sep 12 2026). Here, after the CBS
    # clock fixes and the timeout re-slot, so the rows are in their best order first: measured on the Sep 12 corpus,
    # a timeout ESPN filed 3 minutes early got lowered to the score of the spot it was filed in, not of its clock.
    try:
        for _i, _msg in _lower_spiked_scores(entries, _events).items():
            entry_fixes.setdefault(_i, _msg)
    except Exception as e:
        print(f"WARNING: score spike pass failed for {game_id}: {type(e).__name__}: {e}", flush=True)

    # CBS TAKES OVER THE TAIL of a live game (Roger, Sep 19 2026: "I HAVE to get them this data" - ESPN's play feed sat
    # minutes behind CBS on three licensed games at once and blank on Nebraska). Every CBS play after ESPN's last
    # published play goes in as its own row (cbs_tail.py); the moment ESPN publishes the play, its CBS row leaves the
    # tail and the coach app swaps it for ESPN's. After the score passes (the rows keep CBS's scores), before the keys.
    cbs_tail_added = 0
    cbs_backup_primary = False
    _cbs_doc = None                                       # also what the timeout classifier reads its counters from
    if summary is None:                                   # live or finished: cbs_tail.STATUSES decides
        try:
            import cbs_backup
            import cbs_tail
            _doc = cbs_backup.for_game(_gd, home_team_id, away_team_id, game_id, league)
            _cbs_doc = _doc
            cbs_tail_added, cbs_backup_primary = cbs_tail.append_tail(
                entries, _doc, capp_home, capp_away, game_status)
            if cbs_tail_added:
                print(f"[cbs_tail] {game_id}: {cbs_tail_added} row(s) from CBS past ESPN's last play"
                      f"{' - BACKUP IS THE PRIMARY FEED' if cbs_backup_primary else ''}", flush=True)
        except Exception as e:
            print(f"WARNING: CBS tail failed for {game_id}: {type(e).__name__}: {e}", flush=True)

    # Stable identity per row, AFTER every step that adds rows. Live clients
    # take new plays by this key instead of by count - see _assign_entry_keys.
    _assign_entry_keys(entries)

    # Say who each timeout is actually charged to. ESPN publishes a TV timeout
    # and a team timeout identically, so without this every stoppage is charged
    # and teams sit at zero by the second quarter.
    to_changed, to_examples, to_unresolved = 0, [], []
    # Kickoff date, used to pin the game in the backup source's scoreboard. It is
    # ESPN's UTC stamp, so a late kickoff reads as the next day - the resolver
    # retries on team names alone when the date finds nothing.
    game_date = ""
    try:
        for _c in (data.get("header", {}) or {}).get("competitions", [{}]):
            game_date = _c.get("date", "") or game_date
    except Exception:
        game_date = ""
    try:
        to_changed, to_examples, to_unresolved = _classify_timeouts(
            entries, capp_home, capp_away, league, game_date, cbs_doc=_cbs_doc)
    except Exception as e:
        print(f"WARNING: timeout classify failed for {game_id}: {e}", flush=True)

    # QC-flag remaining issues — operator sees these as red rows in CAPP
    qc_flags = _qc_flag_entries(entries, capp_home, capp_away)
    # A timeout no rule could settle is exactly the row worth a human look, so
    # say so rather than leaving it indistinguishable from a confident one.
    for _i in to_unresolved:
        qc_flags.setdefault(_i, TIMEOUT_UNVERIFIED_NOTE)
    for i, entry in enumerate(entries):
        entry["qc_issue"] = qc_flags.get(i, "")
        if entry.get("cbs_tail"):
            import cbs_tail as _ct
            entry["qc_issue"] = _ct.NOTE                         # a tail row's note is always "backup source"
        elif entry.get("ncaa_status") == "added" and not entry["qc_issue"]:
            entry["qc_issue"] = "Auto-added - check this play"   # Roger, Sep 13 2026: no vendor names on screen
        # The score catch-up note (see _raise_lagging_scores), added here because qc_issue is written over above.
        _sfn = entry.pop("_score_fix_note", "")
        if _sfn and _sfn not in entry["qc_issue"]:
            entry["qc_issue"] = " | ".join([p for p in (entry["qc_issue"], _sfn) if p])
        entry.pop("_forced_key", None)

    auto_fixed_examples = (list(inferred_pat_fixes) + list(entry_fixes.values())
                           + list(to_examples))
    qc_examples = [msg for _, msg in list(qc_flags.items())[:5]]

    # Each quarter judged on its OWN scoreboards (Roger, Sep 14 2026: "Q1 SCOREBOARDS HAVE OVER A 10% ERROR RATE... WOULD
    # YOU LIKE TO USE THE BACKUP SOURCE?" / "look at SMU their 2nd Q Was Trash but the rest was virtually perfect"). An
    # issue is a red row, a row the check fixed or added, or a clock that is still a guess - the definition measured on
    # Sep 12's 125 games (_dev_tools/game_replay_test/measure_quarter_share.py). A crew-replaced row never reaches the
    # coach's screen (ncaa_check.for_keyed_client), so it is not counted.
    quarter_issues = {}
    import ncaa_check as _nc
    for entry in entries:
        if entry.get("ncaa_status") == "superseded" or _nc.after_play_penalty(entry):
            continue                                   # not a row on the coach's screen (ncaa_check.for_keyed_client)
        _qi = quarter_issues.setdefault(str(entry.get("quarter")), {"rows": 0, "issues": 0})
        _qi["rows"] += 1
        _changes = entry.get("ncaa_changes") or []
        _guess = (clock_src.get(str(entry.get("espn_play_id"))) == "guess"
                  and not any(isinstance(c, dict) and c.get("field") == "clock" for c in _changes))
        if is_quarter_issue(entry, _changes, _guess):
            _qi["issues"] += 1
    for _qi in quarter_issues.values():
        _qi["share"] = round(_qi["issues"] / _qi["rows"], 3) if _qi["rows"] else 0.0

    return {
        "entries":    entries,
        "actual_home": actual_home,
        "actual_away": actual_away,
        "home_name":  capp_home,
        "away_name":  capp_away,
        "home_abbrev": home_team_abbrev,
        "away_abbrev": away_team_abbrev,
        # ESPN's team ids + kickoff stamp: how the backup source finds this game (main.py /backup-source).
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "game_date":  game_date,
        "status":     game_status,
        "status_detail": game_detail,
        "league":     league,
        "qc_summary": {
            "auto_fixed_count": len(inferred_pat_fixes) + len(entry_fixes),
            "auto_fixed_examples": auto_fixed_examples[:5],
            "auto_fixed_breakdown": {
                "inferred_missing_pat_count": len(inferred_pat_fixes),
                "post_lag_fix_count": len(entry_fixes),
            },
            "flagged_issue_count": len(qc_flags),
            "flagged_issue_examples": qc_examples,
            "manual_gap_count": inserted_gap_count,
            "ncaa_check": ncaa_summary,
            # Where each play's clock came from (apply_text_snap_clocks): text / espn / kickoff / guess.
            # Roger's Gameday Report alerts when most are guesses - the game needs a Bleacher Report upload.
            "clock_sources": dict(Counter(v or "none" for v in clock_src.values())),
            # {quarter: {rows, issues, share}} - what SBENTRY's end-of-quarter backup prompt reads (see above).
            "quarter_issues": quarter_issues,
            "cbs_tail_rows": cbs_tail_added,
            # True only when CBS has TAKEN OVER (not a hole filled): the coach app paints those rows
            # white and raises its one-time "Now using backup source" alert instead of red.
            "backup_primary": cbs_backup_primary,
        },
        "fetched_at": time.time(),   # unix timestamp — clients poll this to detect changes
    }

# ============================================================
# Live Polling
# ============================================================

def mark_game_active(game_id, league="cfb"):
    """Client heartbeat — 'a user has this game open right now'.

    Called from the /game/{id}/plays and /game/{id}/version endpoints, which
    clients ALREADY poll on a timer while a game window is open. That means no
    client change and no new protocol: ordinary traffic is the signal. The game
    ages out of the active set ACTIVE_GAME_TTL seconds after the last heartbeat,
    so closing the window (or closing CAPP) stops the polling on its own.
    """
    if not game_id:
        return
    now = time.time()
    with _lock:
        entry = _active_games.get(game_id)
        if entry:
            entry["last_seen"] = now
            if league:
                entry["league"] = league
        else:
            _active_games[game_id] = {
                "league":      league or "cfb",
                "last_seen":   now,
                "next_poll":   now,    # someone just asked — poll promptly
                "final_since": 0.0,
                "pre_since":   0.0,
                "done":        False,   # set once a final game has settled
            }


def _schedule_next_poll(game_id, status):
    """Set when this game should next be fetched, based on its own state."""
    now = time.time()
    with _lock:
        entry = _active_games.get(game_id)
        if not entry:
            return
        if status == "in":
            entry["final_since"] = 0.0
            entry["done"] = False
            entry["next_poll"] = now + POLL_LIVE_SECONDS
        elif status == "post":
            # Final: hold briefly in case ESPN posts stat corrections, then stop
            # polling it entirely even if the user leaves the window open.
            if not entry["final_since"]:
                entry["final_since"] = now
            if now - entry["final_since"] >= POST_GRACE_SECONDS:
                # Do NOT delete the entry here. The client is still heartbeating,
                # so a delete just gets re-created as a fresh entry on the next
                # request and re-polled immediately — a finished game would be
                # re-fetched forever at heartbeat rate. Park it instead and let
                # it age out normally when the user closes the window.
                entry["done"] = True
                entry["next_poll"] = now + 10 ** 9
            else:
                entry["next_poll"] = now + POLL_PRE_SECONDS
        else:
            # "pre" (or an errored fetch). Watch for kickoff at 60s while it's
            # plausibly imminent, then back off — so a game opened days early,
            # or a window left open, settles down instead of polling forever.
            if not entry["pre_since"]:
                entry["pre_since"] = now
            imminent = (now - entry["pre_since"]) < PRE_FAST_WINDOW
            entry["next_poll"] = now + (POLL_PRE_SECONDS if imminent
                                        else POLL_PRE_SLOW_SECONDS)


def _evict_caches(now=None):
    """Cache housekeeping. Used to live inside the sweep loop, so with the poller
    dead it never ran and _plays_cache could only grow."""
    now = now or time.time()
    with _lock:
        _post_cutoff = now - 6 * 3600      # completed games older than 6 h
        _any_cutoff = now - 24 * 3600      # any non-live game older than 24 h
        stale = [
            gid for gid, v in _plays_cache.items()
            if (v.get("status") == "post" and v.get("fetched_at", 0) < _post_cutoff)
            or (v.get("status") != "in" and v.get("fetched_at", 0) < _any_cutoff)
        ]
        for gid in stale:
            del _plays_cache[gid]
        # Schedule cache entries are TTL-checked on read, but ones never
        # re-requested would otherwise sit in memory forever.
        _sched_cutoff = now - _SCHEDULE_CACHE_SECONDS
        stale_sched = [k for k, ts in _schedule_ts.items() if ts < _sched_cutoff]
        for k in stale_sched:
            _schedule_cache.pop(k, None)
            _schedule_ts.pop(k, None)


def _poll_loop():
    """Demand-driven poller.

    Polls ONLY games a client currently has open (registered by
    mark_game_active from ordinary client traffic), at a rate set by each
    game's own status:

        "in"   -> every POLL_LIVE_SECONDS (10s)   — full speed
        "pre"  -> every POLL_PRE_SECONDS (60s) while kickoff looks imminent,
                  then POLL_PRE_SLOW_SECONDS
        "post" -> briefly, for stat corrections, then the game is dropped

    With no active games this makes NO ESPN requests at all — it only wakes to
    expire heartbeats and run cache housekeeping. It does not sweep the
    scoreboard and has no notion of "game day"; a real user opening a real game
    is the only thing that ever triggers a fetch.

    (Replaced a loop that hit the ENTIRE CFB+NFL scoreboard every 30s forever,
    plus a per-game play fetch for every live game in the country — ~6,200
    calls/hour at Saturday peak, and 5,760/day even in the off-season.)
    """
    last_housekeeping = 0.0
    while True:
        now = time.time()
        due = []
        with _lock:
            for gid in [g for g, e in _active_games.items()
                        if now - e["last_seen"] > ACTIVE_GAME_TTL]:
                del _active_games[gid]          # nobody is watching this any more
            for gid, entry in _active_games.items():
                if entry["next_poll"] <= now:
                    due.append((gid, entry["league"]))

        if due:
            poll_started = time.time()
            poll_errors = []
            for gid, league in due:
                try:
                    mapped = _fetch_game_plays_mapped(gid, league)
                    with _lock:
                        _plays_cache[gid] = mapped
                    _note_feed_health(gid, mapped)
                    _schedule_next_poll(gid, mapped.get("status", ""))
                except Exception as e:
                    print(f"Live plays error ({gid}): {e}")
                    poll_errors.append(f"Live plays error ({gid}): {e}")
                    _schedule_next_poll(gid, "")   # back off; never hot-loop on errors
            with _lock:
                global _last_poll_started_at, _last_poll_completed_at
                global _last_poll_duration_ms, _last_poll_error
                _last_poll_started_at = poll_started
                _last_poll_completed_at = time.time()
                _last_poll_duration_ms = max(0.0, (_last_poll_completed_at - poll_started) * 1000)
                _last_poll_error = " | ".join(poll_errors[:3])

        if now - last_housekeeping >= 60:
            _evict_caches(now)
            last_housekeeping = now

        # Set once the loop is running. Idle is a HEALTHY state here, so this no
        # longer means "we have polled ESPN" — it means the poller is up.
        _initial_poll_done.set()
        time.sleep(POLLER_TICK)

def start_poller():
    global _poller_thread, _poller_started_at
    with _lock:
        if _poller_thread and _poller_thread.is_alive():
            return False
        _poller_started_at = time.time()
        _poller_thread = threading.Thread(target=_poll_loop, daemon=True, name="capp-espn-poller")
        _poller_thread.start()
    return True


# ── Per-game feed health ──────────────────────────────────────────────────────
# ⚠ WHY THIS EXISTS AND WHY THE EXISTING METRICS WERE NOT ENOUGH.
# Sep 3 2026, Colorado @ Georgia Tech: the game sat "In Progress" at 7:49 of the
# 2nd for 40+ minutes with 0 drives and 0 plays, while two other live games fed
# normally through this same code. get_fetcher_metrics() was GREEN the whole
# time and would have stayed green — it answers "is the poller working", not
# "is THIS GAME producing plays". A coach saw silence and had no way to tell
# whether CAPP was broken. That distinction is the entire point of this block.
#
# Cross-checking CFBD is what makes the answer actionable rather than a shrug:
#   ESPN dark + CFBD has plays  -> the problem is ESPN; a fallback can cover it
#   BOTH dark                   -> nothing is being published for this game at
#                                  the source; the honest answer is "use Manual
#                                  Entry", and no amount of retrying will help.
# Measured that night: Colorado/GT was 0 on BOTH; the two healthy games matched
# closely (ESPN 86 / CFBD 84, ESPN 34 / CFBD 30). So redundancy does NOT rescue
# a dead game feed — do not promise that it does.
_feed_health: dict = {}      # game_id -> tracking dict

# A live game with no new play for this long is stalled. Real football has long
# gaps — TV timeouts, reviews, injuries, halftime — so this is deliberately
# generous. Under it, normal play is silent for ~1-3 minutes.
FEED_STALL_SECONDS = 360          # 6 minutes with zero new plays while "in"
_BREAK_WORDS = re.compile(r"half|end of|delay|suspend|postpon", re.I)   # ESPN's status words for a break in play
FEED_DARK_SECONDS = 300           # "in" this long having produced NOTHING at all


def is_game_watched(game_id) -> bool:
    """Is a client actually holding this game open right now?

    ⚠ POLLING IS DEMAND-DRIVEN. A game is only fetched while a client
    heartbeats it (see mark_game_active). The moment a coach closes the window
    the game ages out and STOPS being polled — that is correct behaviour, not an
    outage, and nothing may raise an alarm about it. get_fetcher_metrics already
    carries this warning for the idle case; feed health and the gameday alerts
    need the same guard or they cry wolf on every game anyone closes.
    """
    with _lock:
        entry = _active_games.get(game_id)
        return bool(entry and not entry.get("done"))


def _note_feed_health(game_id, mapped):
    """Record whether this game is actually producing plays. Called on every
    cache write, both the poller's and the on-demand path."""
    try:
        status = (mapped or {}).get("status", "")
        count = len((mapped or {}).get("entries", []) or [])
        now = time.time()
        with _lock:
            h = _feed_health.get(game_id)
            if not h:
                h = {"first_seen_at": now, "last_change_at": now,
                     "last_count": count, "status": status, "in_since": 0.0}
                _feed_health[game_id] = h
            if status == "in" and not h["in_since"]:
                h["in_since"] = now
            if status != "in":
                h["in_since"] = 0.0
            if count != h["last_count"]:
                h["last_count"] = count
                h["last_change_at"] = now
            h["status"] = status
            h["detail"] = str((mapped or {}).get("status_detail") or "")
            h["checked_at"] = now
    except Exception:
        pass          # health tracking must never break a poll
    try:
        snapshot = get_feed_health(game_id)
        _maybe_alert(game_id, snapshot.get("state", ""), {
            "game_id": game_id,
            "home": (mapped or {}).get("home_name", ""),
            "away": (mapped or {}).get("away_name", ""),
            "status_detail": (mapped or {}).get("status_detail", ""),
            "plays": snapshot.get("plays", 0),
            "quiet_seconds": snapshot.get("seconds_since_new_play", 0),
        })
    except Exception:
        pass


# One-shot alerting. A game going dark must notify ONCE, not on every poll —
# a live game is polled every few seconds, so an un-deduped alert would mean
# hundreds of emails during a single quarter.
_feed_alert_cb = None
_feed_alert_state: dict = {}     # game_id -> last state we alerted on


def set_feed_alert_callback(fn):
    """Registered by main.py. Kept as a hook so this module never imports the
    app — the fetcher must stay importable on its own for tests."""
    global _feed_alert_cb
    _feed_alert_cb = fn


def _maybe_alert(game_id, state, payload):
    """Fire on a TRANSITION only: healthy -> dark/stalled, and back again."""
    if state not in ("dark", "stalled", "healthy"):
        return
    prev = _feed_alert_state.get(game_id)
    if prev == state:
        return
    # Only alert on the first bad state, and on the recovery that follows one.
    if state == "healthy" and prev not in ("dark", "stalled"):
        _feed_alert_state[game_id] = state
        return
    _feed_alert_state[game_id] = state
    cb = _feed_alert_cb
    if not cb:
        return
    try:
        threading.Thread(target=cb, args=(game_id, state, payload), daemon=True).start()
    except Exception as e:
        print(f"feed alert dispatch failed ({game_id}): {e}")


def get_play_data_summary(game_id) -> dict:
    """How good this game's play data is, from the plays ALREADY in the cache - never fetches, never marks the game
    active. For Roger's Gameday Report (Sep 13 2026): review count, NCAA available, how many clocks are guesses."""
    with _lock:
        cached = _plays_cache.get(game_id)
    if not cached:
        return {"cached": False}
    qc = cached.get("qc_summary") or {}
    nc = qc.get("ncaa_check") or {}
    cs = qc.get("clock_sources") or {}
    return {
        "cached": True,
        "status": cached.get("status", ""),
        "plays": len(cached.get("entries") or []),
        "ncaa_available": bool(nc.get("available")),
        "review_count": int(nc.get("review_count") or 0),
        "ncaa_fixed": int(nc.get("fixed") or 0),
        "ncaa_added": int(nc.get("added") or 0),
        "clocks_guessed": int(cs.get("guess") or 0),
        "clocks_total": int(sum(cs.values())) if cs else 0,
        "fetched_at": cached.get("fetched_at"),
    }


def get_feed_health(game_id) -> dict:
    """Is THIS game producing plays? 'healthy' | 'stalled' | 'dark' | 'unknown'.

    Only ever reports a problem for a game ESPN says is IN PROGRESS — a pre-game
    or final game with no new plays is correct, not broken.
    """
    now = time.time()
    with _lock:
        h = dict(_feed_health.get(game_id) or {})
    if not h:
        return {"state": "unknown", "plays": 0, "seconds_since_new_play": 0}
    plays = h.get("last_count", 0)
    quiet = max(0.0, now - h.get("last_change_at", now))
    live_for = max(0.0, now - h["in_since"]) if h.get("in_since") else 0.0
    state = "healthy"
    # A game in a BREAK is quiet on purpose - halftime, the end of a quarter, a weather delay. ESPN's status stays
    # "in" through all of them and its words say which (Roger, Sep 19 2026: "Nebraska says stalled but its halftime
    # for them ... that freaks me out"). No new play in a break is not a stall.
    detail = str(h.get("detail") or "")
    in_break = bool(_BREAK_WORDS.search(detail))
    # Never report a problem for a game nobody has open — it is not being polled
    # BY DESIGN, so "no new plays" says nothing about the feed.
    if h.get("status") == "in" and is_game_watched(game_id) and not in_break:
        if plays == 0 and live_for > FEED_DARK_SECONDS:
            state = "dark"
        elif plays > 0 and quiet > FEED_STALL_SECONDS:
            state = "stalled"
    return {
        "state": state,
        "status": h.get("status", ""),
        "detail": detail,
        "in_break": in_break,
        "watched": is_game_watched(game_id),
        "plays": plays,
        "seconds_since_new_play": int(quiet),
        "live_for_seconds": int(live_for),
    }


def get_fetcher_metrics() -> dict:
    with _lock:
        games_cache = list(_games_cache)
        plays_cache = dict(_plays_cache)
        poller_alive = bool(_poller_thread and _poller_thread.is_alive())
        last_started = _last_poll_started_at
        last_completed = _last_poll_completed_at
        last_duration_ms = _last_poll_duration_ms
        last_error = _last_poll_error
        poller_started_at = _poller_started_at
        active_games = {gid: dict(e) for gid, e in _active_games.items()}

    pending_games = [gid for gid, e in active_games.items() if not e.get("done")]
    live_games = [g for g in games_cache if g.get("status") == "in"]
    final_cached = sum(1 for value in plays_cache.values() if value.get("status") == "post")
    live_cached = sum(1 for value in plays_cache.values() if value.get("status") == "in")

    return {
        "poller_started": poller_started_at > 0,
        "poller_alive": poller_alive,
        "poller_started_at": poller_started_at,
        "initial_poll_complete": _initial_poll_done.is_set(),
        "last_poll_started_at": last_started,
        "last_poll_completed_at": last_completed,
        "last_poll_duration_ms": round(last_duration_ms, 1),
        "last_poll_error": last_error,
        "games_cache_count": len(games_cache),
        "live_games_count": len(live_games),
        "plays_cache_count": len(plays_cache),
        "plays_cache_live_count": live_cached,
        "plays_cache_post_count": final_cached,
        # Demand-driven polling: "idle" means nobody has a game open, which is
        # the CORRECT state most of the year — not an outage. Health checks must
        # not alarm on it.
        # "pending" = games still being polled. A finished game the user hasn't
        # closed yet is tracked but parked, so it must not count as active work.
        "idle": len(pending_games) == 0,
        "active_games_count": len(pending_games),
        "active_game_ids": sorted(pending_games)[:20],
        "tracked_games_count": len(active_games),
    }


def get_game_monitor_rows() -> list:
    with _lock:
        games_cache = list(_games_cache)
        plays_cache = dict(_plays_cache)

    game_lookup = {str(game.get("game_id", "")): game for game in games_cache if game.get("game_id")}
    rows = []
    for game_id, payload in plays_cache.items():
        entries = payload.get("entries", []) or []
        qc_summary = payload.get("qc_summary", {}) or {}
        qc_entries = [entry for entry in entries if entry.get("qc_issue")]
        qc_examples = []
        for entry in qc_entries[:3]:
            issue = str(entry.get("qc_issue", "")).strip()
            if issue:
                qc_examples.append(issue)

        base = game_lookup.get(str(game_id), {})
        try:
            payload_bytes = len(json.dumps(payload).encode("utf-8"))
        except Exception:
            payload_bytes = 0

        fetched_at = float(payload.get("fetched_at", 0) or 0)
        status = payload.get("status") or base.get("status") or "unknown"
        rows.append({
            "game_id": str(game_id),
            "league": payload.get("league") or base.get("league") or "cfb",
            "status": status,
            "home_name": payload.get("home_name") or base.get("home_team") or "",
            "away_name": payload.get("away_name") or base.get("away_team") or "",
            "period": payload.get("period") or base.get("period") or 0,
            "clock": payload.get("clock") or base.get("clock") or "",
            "status_detail": payload.get("status_detail") or base.get("status_detail") or "",
            "fetched_at": fetched_at,
            "age_seconds": round(max(0.0, time.time() - fetched_at), 1) if fetched_at else None,
            "plays_count": len(entries),
            "payload_bytes": payload_bytes,
            "auto_fixed_count": int(qc_summary.get("auto_fixed_count", 0) or 0),
            "auto_fixed_examples": list(qc_summary.get("auto_fixed_examples", []) or []),
            "manual_gap_count": int(qc_summary.get("manual_gap_count", 0) or 0),
            "qc_issue_count": len(qc_entries),
            "qc_examples": qc_examples,
            # Per-game feed state, so a game that is live-but-producing-nothing is
            # visible on the dashboard instead of looking identical to a healthy
            # one. See _note_feed_health for the night that made this necessary.
            "feed_state": (get_feed_health(str(game_id)) or {}).get("state", "unknown"),
            "seconds_since_new_play": (get_feed_health(str(game_id)) or {}).get(
                "seconds_since_new_play", 0),
        })

    # Unhealthy live games first — the whole point is that they stop hiding among
    # the healthy ones.
    rows.sort(key=lambda row: (row["status"] != "in",
                               row.get("feed_state") in ("healthy", "unknown"),
                               -(row["fetched_at"] or 0), row["game_id"]))
    return rows

# ============================================================
# Public API
# ============================================================

def _refresh_live_list_if_stale():
    """The scoreboard is no longer swept in the background, so the live-games
    list is fetched when something actually asks for it and cached briefly.
    Nobody calls /games -> the scoreboard is never hit."""
    global _games_cache_at
    with _lock:
        if (time.time() - _games_cache_at) < LIVE_LIST_TTL:
            return
    # ⚠ THE BARE {} CALL SILENTLY DROPS WHOLE DAYS. Measured Sep 3 2026: ESPN's
    # default scoreboard reported "week 1" and returned Aug 29 + Sep 4-7 —
    # skipping Sep 3, THAT DAY, entirely. 12 games were in progress and this
    # list showed 2. Nine of the ten missing were FBS (Colorado @ Georgia Tech,
    # UMass @ Rutgers, Eastern Illinois @ Minnesota, UAB @ Illinois, Akron @
    # Wake Forest, Bethune-Cookman @ UCF...), so it is NOT a lower-division
    # filter — it is a hole at ESPN's week boundary, and Thursday/Friday games
    # are what fall through it.
    #
    # Fix is additive on purpose: keep the default call (it carries the upcoming
    # week, which the selector relies on) and UNION it with explicit dates for
    # today and tomorrow. Tomorrow is included because ESPN's date buckets are
    # US-Eastern while this server runs UTC, so a night kickoff is already
    # "tomorrow" here. Dedupe by game_id, first writer wins.
    from datetime import datetime, timedelta, timezone
    et_now = datetime.now(timezone.utc) - timedelta(hours=4)   # ET, DST-safe enough for a date bucket
    day_params = [{}] + [{"dates": (et_now + timedelta(days=d)).strftime("%Y%m%d")}
                         for d in (0, 1)]
    new_games = []
    seen_ids = set()
    for lg in ("cfb", "nfl"):
        for params in day_params:
            try:
                for game in _events_to_games(_fetch_scoreboard(lg, params), lg):
                    gid = str(game.get("game_id", ""))
                    if gid and gid in seen_ids:
                        continue
                    if gid:
                        seen_ids.add(gid)
                    new_games.append(game)
            except Exception as e:
                print(f"Live list error ({lg} {params or 'default'}): {e}")
    with _lock:
        _games_cache.clear()
        _games_cache.extend(new_games)
        # Stamped even on failure so an outage can't turn this into a hot retry loop.
        _games_cache_at = time.time()


def get_live_games(league="all", year=None, week=None, seasontype=2):
    if year is not None and week is not None:
        return _fetch_historical_games(league=league, year=year, week=week, seasontype=seasontype)
    _refresh_live_list_if_stale()
    with _lock:
        games = list(_games_cache)
    if league != "all":
        games = [g for g in games if g["league"] == league]
    return games

def get_game_version(game_id):
    """Return the fetched_at timestamp for a cached game without triggering
    a fetch.  Returns 0 if the game is not in cache yet."""
    with _lock:
        cached = _plays_cache.get(game_id)
    return cached.get("fetched_at", 0) if cached else 0

# ── Replay: Simulate a finished game the LIVE way (Step 4, Sep 13 2026) ───────
# The old Simulate drip-fed the FINAL play list, so it never exercised what broke on Sep 12: plays arriving in the order
# the stat crew typed them, late inserts, edits to plays already drawn, NCAA fixes landing later, Refresh. A replay step
# is the game cut off after N typed plays (ESPN play-id order) run through the REAL pipeline - what a live client
# would have received after that play. Same method as _dev_tools/game_replay_test.
# Never marks the game active, never touches the live plays cache or feed-health alerts.
_replay_summaries = {}
_REPLAY_TTL = 3600
_REPLAY_MAX = 20


def _replay_summary(game_id, league):
    now = time.time()
    with _lock:
        hit = _replay_summaries.get((game_id, league))
    if hit and now - hit[0] < _REPLAY_TTL:
        return hit[1]
    url = NFL_SUMMARY_URL if league == "nfl" else CFB_SUMMARY_URL
    r = _session.get(url, params={"event": game_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    with _lock:
        if len(_replay_summaries) >= _REPLAY_MAX:
            _replay_summaries.clear()
        _replay_summaries[(game_id, league)] = (now, data)
    return data


def get_replay_step(game_id, step, league="cfb"):
    """The play list a live client would have had after the step-th typed play. Raises ValueError if the game is
    not final (a replay only exists for a finished game)."""
    import copy as _copy
    from datetime import datetime as _dt
    summ = _replay_summary(game_id, league)
    comps = (summ.get("header") or {}).get("competitions") or [{}]
    if comps[0].get("status", {}).get("type", {}).get("state") != "post":
        raise ValueError("This game is not final yet - a replay is only for a finished game.")
    drives = summ.get("drives", {}) or {}
    all_drives = list(drives.get("previous", []) or []) + ([drives["current"]] if drives.get("current") else [])
    plays = [p for d in all_drives for p in (d.get("plays") or []) if p.get("id")]
    typed = sorted({str(p["id"]) for p in plays}, key=int)
    total = len(typed)
    if not total:
        raise ValueError("This game has no plays to replay.")
    step = max(1, min(int(step), total))
    keep = set(typed[:step])
    trimmed = dict(summ)
    trimmed["drives"] = {"previous": [dict(d, plays=[p for p in (d.get("plays") or []) if str(p.get("id")) in keep])
                                      for d in all_drives]}
    hdr = _copy.deepcopy(summ["header"])
    if step < total:
        hdr["competitions"][0]["status"]["type"]["state"] = "in"
    trimmed["header"] = hdr
    out = _fetch_game_plays_mapped(game_id, league, summary=trimmed)
    gap = 0.0
    if step < total:
        wall = {str(p["id"]): p.get("wallclock") for p in plays}
        try:
            a = _dt.fromisoformat(str(wall[typed[step - 1]]).rstrip("Z"))
            b = _dt.fromisoformat(str(wall[typed[step]]).rstrip("Z"))
            gap = max(0.0, (b - a).total_seconds())
        except Exception:
            gap = 0.0
    out["replay"] = {"step": step, "total": total, "next_gap_s": gap}
    return out


def get_game_plays(game_id, league="cfb", force_refresh=False):
    if force_refresh:
        with _lock:
            _plays_cache.pop(game_id, None)   # evict this game only; all others stay cached
    with _lock:
        cached = _plays_cache.get(game_id)
    if cached:
        return cached
    result = _fetch_game_plays_mapped(game_id, league)
    with _lock:
        _plays_cache[game_id] = result        # cache fresh result for subsequent requests
    _note_feed_health(game_id, result)
    return result


# ── Team list + schedule proxy ────────────────────────────────────────────────
import time as _time

_team_list_cache: dict = {}
_team_list_ts: dict = {}
_TEAM_LIST_CACHE_SECONDS = 86400  # re-fetch team list from ESPN once per day

_schedule_cache: dict = {}
_schedule_ts: dict = {}
_SCHEDULE_CACHE_SECONDS = 86400  # re-fetch a team's schedule from ESPN once per day


def get_team_list(league: str = "cfb") -> list:
    """
    Return raw ESPN team list as [{display_name, id}].
    Cached for 24 h. Client is responsible for name resolution.
    """
    if league in _team_list_cache and _time.time() - _team_list_ts.get(league, 0) < _TEAM_LIST_CACHE_SECONDS:
        return _team_list_cache[league]

    if league == "nfl":
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams"
        params: dict = {"limit": 50}
    else:
        url = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
        params = {"limit": 1000}

    r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    raw_teams = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
    result = [
        {"display_name": t.get("team", {}).get("displayName", ""),
         "id":           t.get("team", {}).get("id", "")}
        for t in raw_teams
        if t.get("team", {}).get("displayName") and t.get("team", {}).get("id")
    ]

    _team_list_cache[league] = result
    _team_list_ts[league] = _time.time()
    return result


def get_team_schedule(team_id: str, season: int = None, league: str = "cfb") -> list:
    """
    Proxy ESPN team schedule endpoint. Returns a list of game dicts with the
    same structure as the games list (game_id, home_team, away_team, status, …)
    plus week and season_type fields. Cached for 1 h per team/season/league.
    """
    cache_key = f"{league}_{team_id}_{season}"
    if cache_key in _schedule_cache and _time.time() - _schedule_ts.get(cache_key, 0) < _SCHEDULE_CACHE_SECONDS:
        return _schedule_cache[cache_key]

    if league == "nfl":
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/schedule"
    else:
        url = f"https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/schedule"

    params: dict = {}
    if season:
        params["season"] = season

    # ⚠ The REGULAR season must be requested EXPLICITLY with seasontype=2.
    # Do NOT go back to the bare default response: ESPN's no-seasontype reply
    # follows the CURRENT phase of the season, so it only *looks* like the
    # regular season during the regular season. Measured Aug 6 2026:
    #   NFL default -> season.type=1, 3 events (the preseason, identical to the
    #                  seasontype=1 fetch below, so dedup dropped it and the
    #                  17 regular-season games vanished entirely)
    #   CFB default -> 0 events (college has no preseason phase at all)
    # Explicit seasontype=2 returns 17 (NFL) and 12 (CFB) as expected, and is
    # phase-independent all year round.
    reg_params = dict(params)
    reg_params["seasontype"] = 2
    r = _session.get(url, params=reg_params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    all_events = list(data.get("events", []))
    # Preseason (NFL only — college has no preseason). Prepended so the
    # schedule reads pre -> reg -> post.
    preseason_ids: set = set()
    if league == "nfl":
        try:
            pre_params = dict(params)
            pre_params["seasontype"] = 1
            pre_r = _session.get(url, params=pre_params, timeout=REQUEST_TIMEOUT)
            if pre_r.ok:
                pre_events = pre_r.json().get("events", [])
                preseason_ids = {e.get("id") for e in pre_events}
                all_events = pre_events + all_events
        except Exception:
            pass

    postseason_ids: set = set()
    try:
        post_params = dict(params)
        post_params["seasontype"] = 3
        post_r = _session.get(url, params=post_params, timeout=REQUEST_TIMEOUT)
        if post_r.ok:
            post_events = post_r.json().get("events", [])
            postseason_ids = {e.get("id") for e in post_events}
            all_events += post_events
    except Exception:
        pass

    games = []
    _seen_event_ids: set = set()
    for event in all_events:
        # Belt-and-braces dedup. The three fetches are now explicitly scoped to
        # seasontype 1/2/3 so they should not overlap, but ESPN has repeated
        # events across phases before (it is what hid the regular season in
        # Aug 2026). Keep the first occurrence only.
        _eid = event.get("id")
        if _eid in _seen_event_ids:
            continue
        _seen_event_ids.add(_eid)

        competition = event.get("competitions", [{}])[0]
        status_obj  = competition.get("status", {})
        state  = status_obj.get("type", {}).get("state", "pre")
        detail = status_obj.get("type", {}).get("shortDetail", "")
        clock  = status_obj.get("displayClock", "0:00")
        period = status_obj.get("period", 0)

        home = away = None
        for competitor in competition.get("competitors", []):
            score_raw = competitor.get("score", 0)
            score = int(score_raw.get("value", 0) if isinstance(score_raw, dict) else score_raw or 0)
            info = {
                "team":    competitor.get("team", {}).get("displayName", ""),
                "abbrev":  competitor.get("team", {}).get("abbreviation", ""),
                "score":   score,
                "team_id": competitor.get("team", {}).get("id", ""),
            }
            if competitor.get("homeAway") == "home":
                home = info
            else:
                away = info

        if not home or not away:
            continue

        conf_name = ""
        if competition.get("conferenceCompetition", False):
            grp = competition.get("groups") or {}
            conf_name = grp.get("shortName") or grp.get("name", "")

        games.append({
            "game_id":      event.get("id", ""),
            "home_team":    home["team"],
            "home_abbrev":  home["abbrev"],
            "home_score":   home["score"],
            "home_team_id": home["team_id"],
            "away_team":    away["team"],
            "away_abbrev":  away["abbrev"],
            "away_score":   away["score"],
            "away_team_id": away["team_id"],
            "status":        state,
            "status_detail": detail,
            "period":        period,
            "clock":         clock,
            "conference":    conf_name,
            "date":          event.get("date", ""),
            "week":          event.get("week", {}).get("number", ""),
            "season_type":   1 if event.get("id") in preseason_ids
                             else 3 if event.get("id") in postseason_ids else 2,
        })

    _schedule_cache[cache_key] = games
    _schedule_ts[cache_key] = _time.time()
    return games
