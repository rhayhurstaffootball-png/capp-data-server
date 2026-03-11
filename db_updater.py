"""
db_updater.py
=============
Runs twice daily. Updates workflow_server.db with latest CFBD data,
then uploads the file to Supabase Storage for clients to download.

Called by APScheduler in main.py, or run manually: python db_updater.py
"""

import sqlite3
import requests
import time
import os
import logging
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
CFBD_KEY      = "vbfhFBcMYIky2ixPSJqCVrmdBYe0Fr4y3ei4kVHypGf1FiQBvoCyGr8vMpduRKOI"
CFBD_BASE     = "https://apinext.collegefootballdata.com"
CFBD_HEADERS  = {"Authorization": f"Bearer {CFBD_KEY}", "Accept": "application/json"}

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_BUCKET = "capp-workflow"
SUPABASE_PATH   = "shared/workflow.db"

DB_PATH = os.path.join(os.path.dirname(__file__), "workflow_server.db")

CURRENT_SEASON = 2026   # Update each year

logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "db_updater.log"),
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("db_updater")

# ── CFBD helpers ──────────────────────────────────────────────────────────────
def cfbd(path, params=None):
    for attempt in range(3):
        try:
            r = requests.get(CFBD_BASE + path, params=params or {},
                             headers=CFBD_HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"CFBD {path} attempt {attempt+1}: {e}")
            time.sleep(2)
    return []

# ── Supabase helpers ──────────────────────────────────────────────────────────
def supabase_headers():
    return {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
    }

def upload_to_supabase(db_path):
    """Upload workflow_server.db to Supabase Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_PATH}"
    with open(db_path, "rb") as f:
        data = f.read()
    headers = {
        **supabase_headers(),
        "Content-Type": "application/octet-stream",
        "x-upsert": "true",   # overwrite if exists
    }
    r = requests.post(url, data=data, headers=headers, timeout=60)
    if r.status_code in (200, 201):
        log.info(f"Uploaded {db_path} to Supabase ({len(data)/1024:.1f} KB)")
        return True
    else:
        log.error(f"Supabase upload failed: {r.status_code} {r.text[:200]}")
        return False

# ── Update logic ──────────────────────────────────────────────────────────────
def update_current_season(conn):
    """
    Pull latest games + results for the current season.
    Updates scores on completed games, inserts new schedule entries.
    """
    cur = conn.cursor()
    updated = 0
    inserted = 0

    for stype in ["regular", "postseason"]:
        games = cfbd("/games", {"year": CURRENT_SEASON, "seasonType": stype})
        time.sleep(0.5)

        for g in games:
            gid       = g.get("id")
            home_pts  = g.get("homePoints")
            away_pts  = g.get("awayPoints")
            completed = g.get("completed", False)
            home      = g.get("homeTeam", "")
            away      = g.get("awayTeam", "")
            home_id   = g.get("homeId")
            away_id   = g.get("awayId")
            date      = g.get("startDate", "")
            week      = str(g.get("week", ""))
            neutral   = g.get("neutralSite", False)
            conf_game = g.get("conferenceGame", False)
            venue     = g.get("venue", "")
            notes     = g.get("notes") or ""

            # Upsert game
            cur.execute("""
                INSERT INTO games (game_id, season, week, season_type, date,
                    home_team, home_team_id, away_team, away_team_id,
                    home_score, away_score, neutral_site, conference_game, venue, notes, completed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    home_score=excluded.home_score,
                    away_score=excluded.away_score,
                    completed=excluded.completed,
                    notes=excluded.notes
            """, (gid, str(CURRENT_SEASON), week, stype, date,
                  home, home_id, away, away_id,
                  home_pts, away_pts, neutral, conf_game, venue, notes, completed))

            if cur.rowcount:
                inserted += 1

            # Upsert schedule rows for each team
            for team, team_id, opp, opp_id, tscore, oscore, ha in [
                (home, home_id, away, away_id, home_pts, away_pts, "home"),
                (away, away_id, home, home_id, away_pts, home_pts, "away"),
            ]:
                result = None
                if tscore is not None and oscore is not None:
                    result = "W" if tscore > oscore else ("L" if tscore < oscore else "T")

                cur.execute("""
                    INSERT INTO schedules (game_id, team, team_id, opponent, opponent_id,
                        season, week, season_type, date, home_away,
                        team_score, opponent_score, result,
                        conference_game, neutral_site, venue)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(game_id, team_id) DO UPDATE SET
                        team_score=excluded.team_score,
                        opponent_score=excluded.opponent_score,
                        result=excluded.result
                """, (gid, team, team_id, opp, opp_id,
                      str(CURRENT_SEASON), week, stype, date, ha,
                      tscore, oscore, result,
                      conf_game, neutral, venue))

                if completed and tscore is not None:
                    updated += 1

    conn.commit()
    log.info(f"Season {CURRENT_SEASON}: {inserted} games upserted, {updated} results updated")
    return inserted, updated

def update_conference_memberships(conn):
    """Refresh team_conferences for current season (handles mid-season moves)."""
    cur = conn.cursor()
    fbs = cfbd("/teams/fbs", {"year": CURRENT_SEASON})
    time.sleep(0.5)

    for t in fbs:
        cur.execute("""
            INSERT INTO team_conferences (team_id, team, season, conference, division, classification)
            VALUES (?, ?, ?, ?, ?, 'fbs')
            ON CONFLICT(team_id, season) DO UPDATE SET
                conference=excluded.conference,
                division=excluded.division
        """, (t["id"], t["school"], CURRENT_SEASON,
              t.get("conference",""), t.get("division","")))

    conn.commit()
    log.info(f"Refreshed {len(fbs)} FBS conference memberships for {CURRENT_SEASON}")

def bump_version(conn, notes=""):
    """Increment db_meta version."""
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
    cur.execute("SELECT version FROM db_meta WHERE id=1")
    version = cur.fetchone()[0]
    log.info(f"DB version bumped to {version}")
    return version

# ── Main entry point ──────────────────────────────────────────────────────────
def run_update():
    """Full update cycle. Called by scheduler or manually."""
    log.info("=== DB update starting ===")
    start = time.time()

    if not os.path.exists(DB_PATH):
        log.error(f"DB not found at {DB_PATH} — run build_server_db.py first")
        return False

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")

        update_conference_memberships(conn)
        ins, upd = update_current_season(conn)
        version = bump_version(conn, f"Games: +{ins} new, {upd} results updated")
        conn.close()

        # Upload to Supabase
        success = upload_to_supabase(DB_PATH)

        elapsed = time.time() - start
        log.info(f"=== Update complete: version {version}, {elapsed:.1f}s ===")
        return success

    except Exception as e:
        log.exception(f"Update failed: {e}")
        return False

if __name__ == "__main__":
    print("Running manual DB update...")
    ok = run_update()
    print("Done." if ok else "Failed — check db_updater.log")
