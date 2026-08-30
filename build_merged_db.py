"""
build_merged_db.py
==================
Builds workflow_server.db by starting from the existing workflow.db and adding:
  1. team_conferences  — season-aware conference membership (2020-2026, from CFBD)
  2. db_meta           — version tracking row
  3. Populates games.result / team_score / opponent_score from CFBD for completed games

The existing tables (teams, games, schedules, name_aliases) are left completely
intact so WFMAIN never needs to change.

Run once to bootstrap. db_updater.py handles ongoing updates.
"""

import sqlite3
import shutil
import requests
import time
import os
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
CFBD_KEY     = "0nmEW1UEpBtWKTuvy7/p6+aOjne6V1IdAQSilX81n10qSEhxrnwE7V8bALRd9Zpg"
CFBD_BASE    = "https://apinext.collegefootballdata.com"
CFBD_HEADERS = {"Authorization": f"Bearer {CFBD_KEY}", "Accept": "application/json"}

SOURCE_DB = os.path.join(os.path.dirname(__file__), "..", "CAPP_FINAL", "WORKFLOW", "workflow.db")
DEST_DB   = os.path.join(os.path.dirname(__file__), "workflow_server.db")
SEASONS   = list(range(2020, 2027))

SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_BUCKET = "capp-workflow"
SUPABASE_PATH   = "shared/workflow.db"

# ── Helpers ───────────────────────────────────────────────────────────────────
def cfbd(path, params=None):
    for attempt in range(3):
        try:
            r = requests.get(CFBD_BASE + path, params=params or {},
                             headers=CFBD_HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  CFBD error ({path}): {e} — attempt {attempt+1}/3")
            time.sleep(2)
    return []

def upload_to_supabase(db_path):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  No Supabase credentials — skipping upload")
        return False
    url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_PATH}"
    with open(db_path, "rb") as f:
        data = f.read()
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/octet-stream",
        "x-upsert": "true",
    }
    r = requests.post(url, data=data, headers=headers, timeout=120)
    if r.status_code in (200, 201):
        print(f"  Uploaded to Supabase ({len(data)/1024:.0f} KB)")
        return True
    else:
        print(f"  Supabase upload failed: {r.status_code} {r.text[:200]}")
        return False

# ── Step 1: Copy source DB ────────────────────────────────────────────────────
def copy_source(source, dest):
    if not os.path.exists(source):
        raise FileNotFoundError(f"Source DB not found: {source}")
    shutil.copy2(source, dest)
    size = os.path.getsize(dest)
    print(f"Copied {source} -> {dest} ({size//1024} KB)")

# ── Step 2: Add team_conferences table ────────────────────────────────────────
def add_team_conferences(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS team_conferences (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id        INTEGER NOT NULL,
            team           TEXT NOT NULL,
            season         INTEGER NOT NULL,
            conference     TEXT,
            division       TEXT,
            classification TEXT,
            UNIQUE(team_id, season)
        )
    """)
    conn.commit()
    print("team_conferences table created.")

def populate_team_conferences(conn):
    cur = conn.cursor()
    total = 0
    for season in SEASONS:
        print(f"  Fetching conference data for {season}...")
        fbs = cfbd("/teams/fbs", {"year": season})
        time.sleep(0.4)

        for t in fbs:
            cur.execute("""
                INSERT INTO team_conferences (team_id, team, season, conference, division, classification)
                VALUES (?, ?, ?, ?, ?, 'fbs')
                ON CONFLICT(team_id, season) DO UPDATE SET
                    conference=excluded.conference,
                    division=excluded.division
            """, (t["id"], t["school"].strip().upper(), season,
                  t.get("conference", ""), t.get("division", "")))
            total += 1

        conn.commit()
        print(f"    {len(fbs)} FBS teams for {season}")

    print(f"team_conferences populated: {total} rows")

# ── Step 3: Add db_meta table ─────────────────────────────────────────────────
def add_db_meta(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS db_meta (
            id         INTEGER PRIMARY KEY CHECK (id = 1),
            version    INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            season     TEXT,
            notes      TEXT
        )
    """)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute("""
        INSERT INTO db_meta (id, version, updated_at, season, notes)
        VALUES (1, 1, ?, '2026', 'Initial build — workflow.db + team_conferences + results')
        ON CONFLICT(id) DO UPDATE SET
            version=excluded.version,
            updated_at=excluded.updated_at,
            notes=excluded.notes
    """, (now,))
    conn.commit()
    print(f"db_meta set to version 1 at {now}")

# ── Step 4: Update game results from CFBD ────────────────────────────────────
def update_game_results(conn):
    """
    Pull completed game results from CFBD and write into the existing games table.
    Only updates rows where game_id matches and result is not already set.
    """
    cur = conn.cursor()
    updated = 0

    for season in SEASONS:
        for stype in ["regular", "postseason"]:
            print(f"  Results {season} {stype}...")
            games = cfbd("/games", {"year": season, "seasonType": stype})
            time.sleep(0.4)

            for g in games:
                if not g.get("completed"):
                    continue
                home_pts = g.get("homePoints")
                away_pts = g.get("awayPoints")
                if home_pts is None or away_pts is None:
                    continue

                game_id    = g.get("id")
                home_cfbd  = g.get("homeTeam", "").strip()
                away_cfbd  = g.get("awayTeam", "").strip()
                date_str   = (g.get("startDate", "") or "")[:10]

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
                """, (home_result, home_pts, away_pts, game_id,
                      date_str, home_cfbd.upper()))
                updated += cur.rowcount

                cur.execute("""
                    UPDATE games SET result=?, team_score=?, opponent_score=?, game_id=?
                    WHERE game_id IS NULL AND date=? AND UPPER(team)=?
                """, (away_result, away_pts, home_pts, game_id,
                      date_str, away_cfbd.upper()))
                updated += cur.rowcount

        conn.commit()

    print(f"Game results updated: {updated} rows")

# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(conn):
    print("\n=== BUILD SUMMARY ===")
    for table in ["teams", "games", "schedules", "team_conferences", "db_meta"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    print("\n  Sample conference history (Liberty — moved conferences):")
    rows = conn.execute("""
        SELECT season, conference FROM team_conferences
        WHERE team='Liberty' ORDER BY season
    """).fetchall()
    for r in rows:
        print(f"    {r[0]}: {r[1]}")

    print("\n  Sample game results (Air Force 2024):")
    rows = conn.execute("""
        SELECT opponent, date, result, team_score, opponent_score
        FROM games WHERE UPPER(team)='AIR FORCE' AND date LIKE '2024%'
        ORDER BY date LIMIT 5
    """).fetchall()
    for r in rows:
        print(f"    {r}")

    print("\n  Games with results populated:")
    print(f"    {conn.execute('SELECT COUNT(*) FROM games WHERE result IS NOT NULL').fetchone()[0]:,} / "
          f"{conn.execute('SELECT COUNT(*) FROM games').fetchone()[0]:,} total")

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Building {DEST_DB}\n")

    copy_source(SOURCE_DB, DEST_DB)

    conn = sqlite3.connect(DEST_DB)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        print("\n--- Adding team_conferences table ---")
        add_team_conferences(conn)
        populate_team_conferences(conn)

        print("\n--- Adding db_meta table ---")
        add_db_meta(conn)

        print("\n--- Updating game results from CFBD ---")
        update_game_results(conn)

        print_summary(conn)
        print("\nDone. workflow_server.db is ready.")

        if SUPABASE_URL:
            print("\n--- Uploading to Supabase ---")
            upload_to_supabase(DEST_DB)
        else:
            print("\nNo SUPABASE_URL set — skipping upload. Run upload manually or set env vars.")

    except Exception as e:
        import traceback
        print(f"\nFATAL: {e}")
        traceback.print_exc()
    finally:
        conn.close()
