"""
db_updater.py
=============
Runs twice daily. Updates workflow_server.db with:
  1. Game results (result, team_score, opponent_score) for completed games
  2. team_conferences for current season (handles mid-season conference changes)
  3. Bumps db_meta.version so clients know to download
  4. Uploads updated DB to Supabase Storage

Does NOT insert new games or schedules — that is handled by the CAPP Toolkit.
Called by APScheduler in main.py, or run manually: python db_updater.py
"""

import sqlite3
import requests
import time
import os
import logging
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

# ── Config ────────────────────────────────────────────────────────────────────
CFBD_KEY      = "vbfhFBcMYIky2ixPSJqCVrmdBYe0Fr4y3ei4kVHypGf1FiQBvoCyGr8vMpduRKOI"
CFBD_BASE     = "https://apinext.collegefootballdata.com"
CFBD_HEADERS  = {"Authorization": f"Bearer {CFBD_KEY}", "Accept": "application/json"}

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_BUCKET = "capp-workflow"
SUPABASE_PATH   = "shared/workflow.db"

DB_PATH = os.path.join(os.path.dirname(__file__), "workflow_server.db")

CURRENT_SEASON = datetime.utcnow().year   # Auto-rolls each year

logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "db_updater.log"),
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("db_updater")

# ── Kickoff date helpers ──────────────────────────────────────────────────────
_ET = ZoneInfo("America/New_York")

def _venue_timezones():
    """venueId -> IANA timezone from CFBD /venues. Missing/failed -> {} (callers
    fall back to Eastern). Used to compute each game's date in the timezone where
    it is actually played."""
    try:
        rows = cfbd("/venues")
        return {v.get("id"): v.get("timezone") for v in rows if v.get("id") and v.get("timezone")}
    except Exception as e:
        log.error(f"_venue_timezones: /venues pull failed ({e}); kickoff dates fall back to Eastern")
        return {}

def _kickoff_local_date(start_date_utc, venue_tz):
    """Correct calendar date for a game from its UTC kickoff.

    CFBD `startDate` is UTC. The old code sliced it (`startDate[:10]`), which put
    every night game on the FOLLOWING day (an 8pm Saturday kickoff is Sunday in
    UTC) — and because dates were also the dedup key, each date shift spawned a
    duplicate row. Rules:
      * A time of exactly midnight Eastern is CFBD's "kickoff time not set yet"
        placeholder — for those the intended date IS the Eastern date (don't shift
        it into the venue timezone or it rolls back a day).
      * A real kickoff uses the VENUE timezone, so the date reflects where the
        game is played (e.g. a late Hawaii or Pacific game stays on its local day).
    """
    s = start_date_utc or ""
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s[:10]
    et = dt.astimezone(_ET)
    if et.hour == 0 and et.minute == 0:          # midnight ET == time TBD -> date only
        return et.date().isoformat()
    tz = ZoneInfo(venue_tz) if venue_tz else _ET
    return dt.astimezone(tz).date().isoformat()

# ── CFBD helpers ──────────────────────────────────────────────────────────────
class CFBDError(Exception):
    """Raised when a CFBD call fails every retry — so callers can tell a real
    failure apart from a genuinely-empty result and NOT silently drop data
    (this is exactly how FCS schedules/results were vanishing: cfbd() returned
    [] on failure, the FCS loop processed 0 rows, and the run reported success)."""


def cfbd(path, params=None, allow_empty=True):
    """Call CFBD with retries + exponential backoff. Larger classification
    pulls (FCS ~850 games) can be slow/rate-limited on the server, so timeout
    is generous and backoff spreads retries. On TOTAL failure, raises CFBDError
    instead of returning [] — callers decide whether an empty pull is
    acceptable (see insert_new_games / update_game_results)."""
    last_err = None
    for attempt in range(5):
        try:
            r = requests.get(CFBD_BASE + path, params=params or {},
                             headers=CFBD_HEADERS, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            log.warning(f"CFBD {path} {params} attempt {attempt+1}/5: {e}")
            time.sleep(min(2 ** attempt, 16))   # 1,2,4,8,16s
    raise CFBDError(f"CFBD {path} {params} failed after 5 attempts: {last_err}")


def _load_alias_map(conn):
    """CFBD name (UPPER) -> canonical teams-table name (UPPER), from the
    existing name_aliases table. Applied on insert/match so e.g. CFBD
    'McNeese' is stored/matched as 'MCNEESE STATE' to align with the teams
    table (previously db_updater stored the raw CFBD name, so aliased FCS
    teams never matched)."""
    cur = conn.cursor()
    amap = {}
    try:
        for alias, team in cur.execute("SELECT alias, team FROM name_aliases"):
            if alias and team:
                amap[_strip_accents(alias).strip().upper()] = _strip_accents(team).strip().upper()
    except Exception as e:
        log.warning(f"Could not load name_aliases: {e}")
    return amap


def _strip_accents(s):
    """Fold accented letters to plain ASCII (é->e, É->E, ñ->n, …) so a team name
    is byte-identical everywhere. The DB, the client's hardcoded team maps, the
    game-selector dropdown, and the schedule query all use plain ASCII; SQLite's
    UPPER()/casefold only fold ASCII, so an accented name ("San José State") never
    matches its ASCII twin and the selector shows no games. Canonicalizing to
    ASCII at the source (here) fixes it for every accented team, present and future
    (e.g. San José State), without per-name special cases."""
    return "".join(c for c in unicodedata.normalize("NFKD", s or "")
                   if not unicodedata.combining(c))

def _canon(name, amap):
    up = _strip_accents(name).strip().upper()
    return amap.get(up, up)

# ── Supabase upload ───────────────────────────────────────────────────────────
def upload_to_supabase(db_path):
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning("No Supabase credentials — skipping upload")
        return False
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_PATH}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/octet-stream",
        "x-upsert": "true",
    }
    size = os.path.getsize(db_path)
    with open(db_path, "rb") as f:
        r = requests.post(url, data=f, headers=headers, timeout=120)
    if r.status_code in (200, 201):
        log.info(f"Uploaded to Supabase ({size//1024} KB)")
        return True
    else:
        log.error(f"Supabase upload failed: {r.status_code} {r.text[:200]}")
        return False

# ── Insert new schedule games ─────────────────────────────────────────────────
def insert_new_games(conn):
    """
    Pull the current season schedule from CFBD and insert any games not already
    in the DB. Inserts one row per team in games, and one row per team in schedules.
    Team names stored UPPERCASE to match existing data convention.
    """
    cur = conn.cursor()
    amap = _load_alias_map(conn)
    vtz = _venue_timezones()          # venueId -> IANA tz (for venue-local dates)
    games_added = 0
    schedules_added = 0

    for classification in ["fbs", "fcs"]:
      for stype in ["regular", "postseason"]:
        try:
            games = cfbd("/games", {"year": CURRENT_SEASON, "seasonType": stype, "classification": classification})
        except CFBDError as e:
            # Loud, but non-fatal per-classification: a failed FCS pull must
            # not block FBS or halt the whole run. Existing rows are retained
            # (inserts are if-not-exists), so nothing is lost — just not
            # refreshed this cycle. This is the fix for FCS silently vanishing.
            log.error(f"insert_new_games: {classification}/{stype} pull FAILED — skipping this run: {e}")
            continue
        time.sleep(0.5)

        for g in games:
            game_id    = g.get("id")
            home_cfbd  = _canon(g.get("homeTeam", ""), amap)
            away_cfbd  = _canon(g.get("awayTeam", ""), amap)
            # Venue-local kickoff date (NOT the raw UTC slice — see _kickoff_local_date).
            date_str   = _kickoff_local_date(g.get("startDate", ""), vtz.get(g.get("venueId")))
            week       = str(g.get("week", ""))
            venue      = g.get("venue", "") or ""
            neutral    = g.get("neutralSite", False)
            conf_game  = g.get("conferenceGame", False)
            home_pts   = g.get("homePoints")
            away_pts   = g.get("awayPoints")
            completed  = g.get("completed", False)

            # Results if game is completed
            home_result = None
            away_result = None
            if completed and home_pts is not None and away_pts is not None:
                home_result = "W" if home_pts > away_pts else ("L" if home_pts < away_pts else "T")
                away_result = "W" if away_pts > home_pts else ("L" if away_pts < home_pts else "T")

            # Insert into games table — one row per team. Keyed on (team, game_id):
            # update the existing row in place (incl. a corrected date) so a shifted
            # kickoff date can never spawn a duplicate row. Only genuinely-new games
            # are inserted. (game_id is unique per team per season.)
            for team, opp, tscore, oscore, result in [
                (home_cfbd, away_cfbd, home_pts, away_pts, home_result),
                (away_cfbd, home_cfbd, away_pts, home_pts, away_result),
            ]:
                if game_id is not None:
                    cur.execute("""
                        UPDATE games SET
                            opponent=?, date=?, home_team=?, away_team=?, venue=?,
                            neutral_site=?, conference_game=?, week=?,
                            result=?, team_score=?, opponent_score=?
                        WHERE team=? AND game_id=? AND season=?
                    """, (opp, date_str, home_cfbd, away_cfbd, venue,
                          neutral, conf_game, week, result, tscore, oscore,
                          team, game_id, str(CURRENT_SEASON)))
                    if cur.rowcount:
                        games_added += 1
                        continue
                cur.execute("""
                    INSERT INTO games
                        (team, opponent, date, home_team, away_team, venue,
                         neutral_site, conference_game, week, season, game_id,
                         result, team_score, opponent_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(team, opponent, date) DO UPDATE SET
                        game_id=excluded.game_id,
                        result=excluded.result,
                        team_score=excluded.team_score,
                        opponent_score=excluded.opponent_score
                """, (team, opp, date_str, home_cfbd, away_cfbd, venue,
                      neutral, conf_game, week, str(CURRENT_SEASON), game_id,
                      result, tscore, oscore))
                if cur.rowcount:
                    games_added += 1

            # Insert into schedules — one row per team. Also keyed on (team, game_id)
            # so a corrected date updates the existing row instead of adding a dup.
            for team, opp, loc in [
                (home_cfbd, away_cfbd, "vs"),
                (away_cfbd, home_cfbd, "at"),
            ]:
                existing = None
                if game_id is not None:
                    existing = cur.execute("""
                        SELECT id FROM schedules
                        WHERE team=? AND game_id=? AND season=?
                    """, (team, game_id, str(CURRENT_SEASON))).fetchone()

                if existing:
                    cur.execute("""
                        UPDATE schedules SET
                            opponent=?, date=?, location=?, home_team=?, away_team=?, week=?
                        WHERE id=?
                    """, (opp, date_str, loc, home_cfbd, away_cfbd, week, existing[0]))
                else:
                    cur.execute("""
                        INSERT INTO schedules
                            (team, opponent, date, location, home_team, away_team,
                             season, game_id, week)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (team, opp, date_str, loc, home_cfbd, away_cfbd,
                          str(CURRENT_SEASON), game_id, week))
                    schedules_added += 1

    conn.commit()
    log.info(f"Season {CURRENT_SEASON}: {games_added} game rows, {schedules_added} schedule rows inserted/updated (FBS+FCS)")
    return games_added, schedules_added


# ── Update game results ───────────────────────────────────────────────────────
def update_game_results(conn):
    """
    Pull completed game results from CFBD and write into the existing games table.
    Two-pass matching to handle both title case (game_id rows) and
    uppercase (older toolkit rows) team names.
    """
    cur = conn.cursor()
    amap = _load_alias_map(conn)
    updated = 0

    for classification in ["fbs", "fcs"]:
      for stype in ["regular", "postseason"]:
        try:
            games = cfbd("/games", {"year": CURRENT_SEASON, "seasonType": stype, "classification": classification})
        except CFBDError as e:
            log.error(f"update_game_results: {classification}/{stype} pull FAILED — skipping this run: {e}")
            continue
        time.sleep(0.5)

        for g in games:
            if not g.get("completed"):
                continue
            home_pts = g.get("homePoints")
            away_pts = g.get("awayPoints")
            if home_pts is None or away_pts is None:
                continue

            game_id   = g.get("id")
            # Canonicalize via name_aliases so results match the rows
            # insert_new_games created (which now use aliased UPPER names).
            home_cfbd = _canon(g.get("homeTeam", ""), amap)
            away_cfbd = _canon(g.get("awayTeam", ""), amap)
            date_str  = (g.get("startDate", "") or "")[:10]

            home_result = "W" if home_pts > away_pts else ("L" if home_pts < away_pts else "T")
            away_result = "W" if away_pts > home_pts else ("L" if away_pts < home_pts else "T")

            # Pass 1: match by game_id (title case rows imported directly from CFBD)
            cur.execute("""
                UPDATE games SET result=?, team_score=?, opponent_score=?
                WHERE game_id=? AND team=?
            """, (home_result, home_pts, away_pts, game_id, home_cfbd))
            updated += cur.rowcount

            cur.execute("""
                UPDATE games SET result=?, team_score=?, opponent_score=?
                WHERE game_id=? AND team=?
            """, (away_result, away_pts, home_pts, game_id, away_cfbd))
            updated += cur.rowcount

            # Pass 2: match by UPPER(team) + date (uppercase rows without game_id)
            cur.execute("""
                UPDATE games SET result=?, team_score=?, opponent_score=?, game_id=?
                WHERE game_id IS NULL AND date=? AND UPPER(team)=?
            """, (home_result, home_pts, away_pts, game_id, date_str, home_cfbd.upper()))
            updated += cur.rowcount

            cur.execute("""
                UPDATE games SET result=?, team_score=?, opponent_score=?, game_id=?
                WHERE game_id IS NULL AND date=? AND UPPER(team)=?
            """, (away_result, away_pts, home_pts, game_id, date_str, away_cfbd.upper()))
            updated += cur.rowcount

    conn.commit()
    log.info(f"Season {CURRENT_SEASON}: {updated} game result rows updated")
    return updated

# ── Update conference memberships ─────────────────────────────────────────────
def update_conference_memberships(conn):
    """
    Refresh team_conferences for the current season.
    Handles mid-season conference changes and new teams.
    Team names stored UPPERCASE to match the rest of the DB.
    """
    cur = conn.cursor()
    try:
        fbs = cfbd("/teams/fbs", {"year": CURRENT_SEASON})
        time.sleep(0.5)
    except CFBDError as e:
        log.error(f"update_conference_memberships: FBS teams pull FAILED — skipping: {e}")
        fbs = []
    try:
        fcs = cfbd("/teams", {"year": CURRENT_SEASON, "classification": "fcs"})
        time.sleep(0.5)
    except CFBDError as e:
        log.error(f"update_conference_memberships: FCS teams pull FAILED — skipping: {e}")
        fcs = []

    for classification, teams in [("fbs", fbs), ("fcs", fcs)]:
        for t in teams:
            cur.execute("""
                INSERT INTO team_conferences (team_id, team, season, conference, division, classification)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(team_id, season) DO UPDATE SET
                    conference=excluded.conference,
                    division=excluded.division
            """, (t["id"], t["school"].strip().upper(), CURRENT_SEASON,
                  t.get("conference", ""), t.get("division", ""), classification))

    conn.commit()
    log.info(f"Refreshed {len(fbs)} FBS + {len(fcs)} FCS conference memberships for {CURRENT_SEASON}")

# ── Bump version ──────────────────────────────────────────────────────────────
def bump_version(conn, notes=""):
    cur = conn.cursor()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    cur.execute("""
        UPDATE db_meta SET
            version    = version + 1,
            updated_at = ?,
            season     = ?,
            notes      = ?
        WHERE id = 1
    """, (now, str(CURRENT_SEASON), notes or f"Auto-update {now}"))
    conn.commit()
    version = cur.execute("SELECT version FROM db_meta WHERE id=1").fetchone()[0]
    log.info(f"DB version bumped to {version}")
    return version

# ── Main entry point ──────────────────────────────────────────────────────────
def run_update():
    """Full update cycle. Called by scheduler or manually."""
    log.info("=== DB update starting ===")
    start = time.time()

    if not os.path.exists(DB_PATH):
        log.error(f"DB not found at {DB_PATH} — run build_merged_db.py first")
        return False

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")

        update_conference_memberships(conn)
        g_added, s_added = insert_new_games(conn)
        updated = update_game_results(conn)

        if not (g_added or s_added or updated):
            conn.close()
            elapsed = time.time() - start
            log.info(f"=== No changes — skipping version bump and upload ({elapsed:.1f}s) ===")
            return True

        version = bump_version(conn, f"+{g_added} games, +{s_added} schedules, {updated} results updated")
        conn.close()

        success = upload_to_supabase(DB_PATH)

        elapsed = time.time() - start
        log.info(f"=== Update complete: version {version}, {elapsed:.1f}s ===")
        return success

    except Exception as e:
        log.exception(f"Update failed: {e}")
        return False

if __name__ == "__main__":
    print(f"Running manual DB update for season {CURRENT_SEASON}...")
    ok = run_update()
    if ok:
        print("Done — DB updated and uploaded to Supabase.")
    elif not SUPABASE_URL:
        print("DB updated locally. No SUPABASE_URL set — upload skipped (normal for local dev).")
    else:
        print("Failed — check db_updater.log")
