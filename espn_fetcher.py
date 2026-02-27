"""
CAPP Data Server - ESPN Fetcher with Full Play Mapping Pipeline
Ports the complete mapping logic from CAPP's espn_live.py so the server
returns fully CAPP-ready play entries to clients.
"""

import requests
import threading
import time

# ============================================================
# ESPN API URLs
# ============================================================
CFB_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
NFL_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
CFB_SUMMARY_URL    = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary"
NFL_SUMMARY_URL    = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"

REQUEST_TIMEOUT = 15
POLL_INTERVAL   = 30

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
    "Saint Francis (PA)": "Saint Francis (PA)",
    "Saint Francis Red Flash": "Saint Francis (PA)",
    "Delaware Blue Hens": "Delaware",
    "Long Island University Sharks": "LIU",
    "Grambling Tigers": "Grambling State",
    "SE Louisiana Lions": "Southeastern Louisiana",
    "UAlbany Great Danes": "Albany",
    "East Texas A&M Lions": "Texas A&M-Commerce",
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
    "Dayton", "Delaware State", "Drake", "Duquesne", "East Tennessee State",
    "Eastern Illinois", "Eastern Kentucky", "Eastern Washington", "Elon",
    "Florida A&M", "Fordham", "Furman", "Gardner-Webb", "Georgetown",
    "Grambling State", "Hampton", "Harvard", "Holy Cross", "Houston Baptist",
    "Houston Christian", "Howard", "Idaho", "Idaho State", "Illinois State",
    "Incarnate Word", "Indiana State", "Jackson State", "LIU", "Lafayette",
    "Lamar", "Lehigh", "Lindenwood", "Maine", "Marist", "McNeese",
    "Mercer", "Mercyhurst", "Merrimack", "Mississippi Valley State",
    "Missouri State", "Monmouth", "Montana", "Montana State", "Morehead State",
    "Morgan State", "Murray State", "New Hampshire", "Nicholls", "Norfolk State",
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
    "Villanova", "Wagner", "Weber State", "Western Carolina", "Western Illinois",
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

# ============================================================
# Team Name Utilities
# ============================================================

def espn_name_to_capp_name(espn_display_name, league="cfb"):
    if not espn_display_name:
        return None
    name = espn_display_name.strip()
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
    "kickoff": 5, "kickoff return": 5, "timeout": 0,
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
        if fp == 0 and prev_fp != 0:
            if prev_fp < 0:
                prev_yte = 100 + prev_fp
            else:
                prev_yte = prev_fp
            new_yte = max(0, min(100, prev_yte - prev_gain))
            entry["field_position"] = convert_field_position(new_yte)
        prev_fp = entry.get("field_position", 0)
        prev_gain = int(entry.get("gain", 0))

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
# Scoring Gap Detection (period boundaries)
# ============================================================

def _fill_scoring_gaps(entries, home_display, away_display):
    """
    After scoreboard lag: detect period-opening KOs where the score
    jumped vs. the previous period's last entry — indicating ESPN
    omitted the scoring play(s) from its play-by-play feed entirely.

    This happens when a TD (and sometimes EP) occurs on a play ESPN
    tags as "End Period" or "End of Half", which our pipeline filters.
    The score jump then appears silently on the Q3/Q4 opening KO.

    Inserts synthetic TD + EP/2PT placeholder entries BEFORE the KO
    so the operator sees them as red "Manual entry required" rows in
    SBENTRY and knows to enter the actual play data.

    Also updates the KO entry's lag score to the correct post-scoring
    value so the rest of the scoreboard remains accurate.

    Only handles TD-range deltas (6, 7, 8). Other deltas are left for
    _qc_flag_entries to handle.

    Modifies entries in-place. Returns number of gaps patched.
    """
    _MANUAL_QC = "Manual entry required — scoring play not in ESPN feed"
    _TD_DELTAS  = {6, 7, 8}
    gaps_found  = 0
    i = 1
    while i < len(entries):
        entry = entries[i]
        prev  = entries[i - 1]

        # Only period-opening KO entries
        if str(entry.get("down", "")) != "KO":
            i += 1
            continue
        if str(entry.get("quarter", "")) == str(prev.get("quarter", "")):
            i += 1
            continue

        # After lag: entry[i] shows last-period end score;
        # entry[i+1] shows ESPN's post-KO score (includes filtered scoring).
        if i + 1 >= len(entries):
            i += 1
            continue

        nxt = entries[i + 1]
        dh = nxt["home_score"] - entry["home_score"]
        da = nxt["away_score"] - entry["away_score"]
        total_delta = max(abs(dh), abs(da))

        if total_delta == 0 or total_delta not in _TD_DELTAS:
            i += 1
            continue  # No gap, or non-TD delta — skip

        gaps_found += 1

        # Score before the missing TD (= KO's current lag score = period end)
        td_h = entry["home_score"]
        td_a = entry["away_score"]

        # Score after TD (always +6, regardless of EP outcome)
        after_td_h = td_h + (6 if dh > 0 else 0)
        after_td_a = td_a + (6 if da > 0 else 0)

        # EP or 2PT based on total delta
        pat_down = "2PT" if total_delta == 8 else "EP"

        # Score after full sequence (what the KO should show as "before play")
        after_pat_h = td_h + dh
        after_pat_a = td_a + da

        # Possession = scoring team (best guess; operator may correct)
        scoring_team = home_display if dh > 0 else away_display

        # Synthetic entries belong to the previous period at 0:00
        prev_qtr = str(prev.get("quarter", entry.get("quarter", "1")))

        td_entry = {
            "quarter":        prev_qtr,
            "clock":          "0:00",
            "down":           "1",        # unknown — operator must correct
            "distance":       0,
            "field_position": 0,
            "gain":           6,
            "home_score":     td_h,
            "away_score":     td_a,
            "possession":     scoring_team,
            "home_time_out":  "No",
            "away_time_out":  "No",
            "run_clock":      "No",
            "play_text":      "Scoring play — data not in ESPN feed (enter manually)",
            "wallclock":      "",
            "qc_issue":       _MANUAL_QC,
        }

        pat_entry = {
            "quarter":        prev_qtr,
            "clock":          "0:00",
            "down":           pat_down,
            "distance":       3,
            "field_position": 3,
            "gain":           0,
            "home_score":     after_td_h,
            "away_score":     after_td_a,
            "possession":     scoring_team,
            "home_time_out":  "No",
            "away_time_out":  "No",
            "run_clock":      "No",
            "play_text":      f"{pat_down} — data not in ESPN feed (enter manually)",
            "wallclock":      "",
            "qc_issue":       _MANUAL_QC,
        }

        # Update the KO's lag score to the correct post-scoring value
        entry["home_score"] = after_pat_h
        entry["away_score"] = after_pat_a

        # Insert synthetic entries before the KO (at position i)
        entries.insert(i, pat_entry)
        entries.insert(i, td_entry)

        # Skip past: synthetic TD, synthetic EP/2PT, and updated KO
        i += 3

    return gaps_found


# ============================================================
# QC Flagging
# ============================================================

_QC_VALID_POS    = {0, 1, 2, 3, 6, 7, 8}   # valid positive score deltas
_QC_BUNDLED_ART  = {-7, -8}                 # lag mirrors of bundled TD+EP — skip
_QC_STUCK_THRESH = 4

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

    # Stuck clock (4+ consecutive same clock in same quarter, non-special down)
    streak = 1
    for i in range(1, len(entries)):
        c, p = entries[i], entries[i - 1]
        if (c.get("clock") == p.get("clock")
                and c.get("quarter") == p.get("quarter")
                and str(c.get("down", "")) not in ("KO", "EP", "2PT")):
            streak += 1
            if streak == _QC_STUCK_THRESH:
                flags.setdefault(i, []).append(f"Clock stuck ({streak}+ plays)")
        else:
            streak = 1

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
                elif delta == 8:
                    play["point_after_attempt"] = {"text": "Two-Point Conversion", "value": 2}
                elif delta == 6:
                    play["point_after_attempt"] = {"text": "Extra Point Attempt - No Good", "value": 0}
        prev_home = curr_home
        prev_away = curr_away


def _parse_play(play, drive_team_id, home_team_id, away_team_id):
    play_id = str(play.get("id", ""))
    if not play_id:
        return None
    play_type = play.get("type", {})
    type_text = play_type.get("text", "")
    type_id = int(play_type.get("id", 0))
    _skip = type_text.lower()
    if _skip in ("end period", "end of half", "end of game", "coin toss",
                 "two-minute warning", "officials time out"):
        return None
    desc_text = play.get("text", "").lower()
    if "official timeout" in desc_text or "officials time out" in desc_text:
        return None
    clock_obj = play.get("clock", {})
    clock_display = clock_obj.get("displayValue", "0:00")
    period_num = int(play.get("period", {}).get("number", 1))
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
    home_score = int(play.get("homeScore", 0))
    away_score = int(play.get("awayScore", 0))
    text = play.get("text", "")
    sequence_number = int(play.get("sequenceNumber", "0"))
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
    elif is_timeout:
        down = str(start_down) if start_down else "1"
        distance = start_distance if start_distance else 10
        gain = 0
    else:
        down = str(start_down) if start_down else "1"
        distance = start_distance if start_distance else 10
        gain = stat_yardage

    run_clock = "No"
    if is_rush and not scoring and not is_kickoff:
        end_down = play.get("end_down")
        if end_down != 1 and gain < distance:
            run_clock = "Yes"

    home_time_out = away_time_out = "No"
    if is_timeout:
        desc_lower = description.lower()
        if home_team_abbrev and home_team_abbrev.lower() in desc_lower:
            home_time_out = "Yes"
        elif away_team_abbrev and away_team_abbrev.lower() in desc_lower:
            away_time_out = "Yes"
        elif home_team_display and home_team_display.lower() in desc_lower:
            home_time_out = "Yes"
        elif away_team_display and away_team_display.lower() in desc_lower:
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
    }
    results.append(entry)

    if pat and not is_kickoff and not is_field_goal and not is_extra_point and not is_two_point:
        pat_text = pat.get("text", "").lower()
        pat_value = pat.get("value", 0)
        is_two_point_pat = "two" in pat_text or "2pt" in pat_text or "2-point" in pat_text or pat_value == 2
        pat_entry = {
            "home_score": home_score,
            "away_score": away_score,
            "clock": clock,
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

def _fetch_historical_games(league, year, week, seasontype=2):
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
    return results

# ============================================================
# Play Fetching + Full Mapping Pipeline
# ============================================================

def _fetch_game_plays_mapped(game_id, league="cfb"):
    url = NFL_SUMMARY_URL if league == "nfl" else CFB_SUMMARY_URL
    r = _session.get(url, params={"event": game_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

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
    for comp in header.get("competitions", [{}]):
        game_status = comp.get("status", {}).get("type", {}).get("state", "in")

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
    _infer_missing_pats(all_plays)
    # Annotate which team scored each TD (uses score deltas, not drive_team_id)
    _annotate_td_scoring_teams(all_plays)

    # Fix clocks, estimate snap times
    fix_clock_anomalies(all_plays)
    estimate_snap_clocks(all_plays)

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
    _auto_fix_entries(entries)

    # Insert placeholder entries for TD/EP plays ESPN omitted from the feed
    # (e.g., last-second Q2 TDs filtered as "End Period" type plays)
    _fill_scoring_gaps(entries, capp_home, capp_away)

    # QC-flag remaining issues — operator sees these as red rows in CAPP
    qc_flags = _qc_flag_entries(entries, capp_home, capp_away)
    for i, entry in enumerate(entries):
        entry["qc_issue"] = qc_flags.get(i, "")

    return {
        "entries": entries,
        "actual_home": actual_home,
        "actual_away": actual_away,
        "home_name": capp_home,
        "away_name": capp_away,
        "home_abbrev": home_team_abbrev,
        "away_abbrev": away_team_abbrev,
        "status": game_status,
        "league": league,
    }

# ============================================================
# Live Polling
# ============================================================

def _poll_loop():
    while True:
        new_games = []
        for league in ["cfb", "nfl"]:
            try:
                events = _fetch_scoreboard(league, {})
                games = _events_to_games(events, league)
                new_games.extend(games)
                for g in games:
                    if g["status"] == "in":
                        try:
                            mapped = _fetch_game_plays_mapped(g["game_id"], league)
                            with _lock:
                                _plays_cache[g["game_id"]] = mapped
                        except Exception as e:
                            print(f"Live plays error ({g['game_id']}): {e}")
            except Exception as e:
                print(f"Poll error ({league}): {e}")

        with _lock:
            _games_cache.clear()
            _games_cache.extend(new_games)

        time.sleep(POLL_INTERVAL)

def start_poller():
    t = threading.Thread(target=_poll_loop, daemon=True)
    t.start()

# ============================================================
# Public API
# ============================================================

def get_live_games(league="all", year=None, week=None, seasontype=2):
    if year is not None and week is not None:
        return _fetch_historical_games(league=league, year=year, week=week, seasontype=seasontype)
    with _lock:
        games = list(_games_cache)
    if league != "all":
        games = [g for g in games if g["league"] == league]
    return games

def get_game_plays(game_id, league="cfb"):
    with _lock:
        cached = _plays_cache.get(game_id)
    if cached:
        return cached
    return _fetch_game_plays_mapped(game_id, league)
