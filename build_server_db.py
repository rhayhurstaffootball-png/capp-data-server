"""
build_server_db.py
==================
Builds workflow_server.db from scratch using CFBD data.
Pulls seasons 2020-2026 for:
  - team_conferences (season-aware conference membership)
  - games + schedules (with scores/results where available)
  - teams (canonical team list)
  - name_aliases (preserved from existing logic)

Run once to bootstrap. db_updater.py handles ongoing updates.
DO NOT run against local workflow.db — this creates a separate server DB.
"""

import sqlite3
import requests
import time
import os
from datetime import datetime

CFBD_KEY  = "vbfhFBcMYIky2ixPSJqCVrmdBYe0Fr4y3ei4kVHypGf1FiQBvoCyGr8vMpduRKOI"
CFBD_BASE = "https://apinext.collegefootballdata.com"
HEADERS   = {"Authorization": f"Bearer {CFBD_KEY}", "Accept": "application/json"}

DB_PATH   = os.path.join(os.path.dirname(__file__), "workflow_server.db")
SEASONS   = list(range(2020, 2027))

def cfbd(path, params=None):
    """Make a CFBD API call with retry."""
    for attempt in range(3):
        try:
            r = requests.get(CFBD_BASE + path, params=params or {}, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  CFBD error ({path}): {e} — attempt {attempt+1}/3")
            time.sleep(2)
    return []

def create_schema(conn):
    """Create all tables."""
    cur = conn.cursor()

    # Canonical team list (latest snapshot — conference reflects most recent season)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            team       TEXT NOT NULL,
            conference TEXT,
            division   TEXT,
            team_id    INTEGER UNIQUE
        )
    """)

    # Season-aware conference membership — THE key new table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_conferences (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id      INTEGER NOT NULL,
            team         TEXT NOT NULL,
            season       INTEGER NOT NULL,
            conference   TEXT,
            division     TEXT,
            classification TEXT,
            UNIQUE(team_id, season)
        )
    """)

    # Games with scores + results
    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id           INTEGER UNIQUE,
            season            TEXT,
            week              TEXT,
            season_type       TEXT,
            date              TEXT,
            home_team         TEXT,
            home_team_id      INTEGER,
            away_team         TEXT,
            away_team_id      INTEGER,
            home_score        INTEGER,
            away_score        INTEGER,
            neutral_site      BOOLEAN,
            conference_game   BOOLEAN,
            venue             TEXT,
            notes             TEXT,
            completed         BOOLEAN DEFAULT 0
        )
    """)

    # Schedules view-style table (team-centric rows, one per team per game)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id           INTEGER,
            team              TEXT,
            team_id           INTEGER,
            opponent          TEXT,
            opponent_id       INTEGER,
            season            TEXT,
            week              TEXT,
            season_type       TEXT,
            date              TEXT,
            home_away         TEXT,
            team_score        INTEGER,
            opponent_score    INTEGER,
            result            TEXT,
            conference_game   BOOLEAN,
            neutral_site      BOOLEAN,
            venue             TEXT,
            team_division     TEXT,
            opponent_division TEXT,
            UNIQUE(game_id, team_id)
        )
    """)

    # Name aliases for fuzzy matching
    cur.execute("""
        CREATE TABLE IF NOT EXISTS name_aliases (
            alias TEXT PRIMARY KEY,
            team  TEXT NOT NULL
        )
    """)

    # Version tracking — the heartbeat of the update system
    cur.execute("""
        CREATE TABLE IF NOT EXISTS db_meta (
            id         INTEGER PRIMARY KEY CHECK (id = 1),
            version    INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL,
            season     TEXT,
            notes      TEXT
        )
    """)

    conn.commit()
    print("Schema created.")

def populate_team_conferences(conn):
    """Pull team/conference data per season from CFBD."""
    cur = conn.cursor()
    team_master = {}  # team_id -> latest team info

    for season in SEASONS:
        print(f"\n  Season {season}...")

        # FBS teams
        fbs = cfbd("/teams/fbs", {"year": season})
        time.sleep(0.5)

        # All teams (includes FCS)
        all_teams = cfbd("/teams", {"year": season})
        time.sleep(0.5)

        # Build a lookup by id for all teams this season
        season_teams = {t["id"]: t for t in all_teams}

        # Merge FBS (has year-specific conference data)
        for t in fbs:
            season_teams[t["id"]] = t

        inserted = 0
        for tid, t in season_teams.items():
            school = t.get("school", "")
            conf   = t.get("conference") or ""
            div    = t.get("division") or ""
            cls    = t.get("classification") or ""

            cur.execute("""
                INSERT INTO team_conferences (team_id, team, season, conference, division, classification)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(team_id, season) DO UPDATE SET
                    conference=excluded.conference,
                    division=excluded.division,
                    classification=excluded.classification
            """, (tid, school, season, conf, div, cls))

            # Track latest snapshot for teams table
            team_master[tid] = {"team": school, "conference": conf, "division": div, "team_id": tid}
            inserted += 1

        print(f"    {inserted} teams for {season}")

    # Populate teams table from latest season snapshot (2026)
    print("\n  Populating teams table from latest season...")
    for tid, info in team_master.items():
        cur.execute("""
            INSERT INTO teams (team, conference, division, team_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(team_id) DO UPDATE SET
                team=excluded.team,
                conference=excluded.conference,
                division=excluded.division
        """, (info["team"], info["conference"], info["division"], info["team_id"]))

    conn.commit()
    print(f"  Teams table: {len(team_master)} teams")

def populate_games(conn):
    """Pull all games + results per season from CFBD."""
    cur = conn.cursor()

    for season in SEASONS:
        for stype in ["regular", "postseason"]:
            print(f"\n  Games {season} {stype}...")
            games = cfbd("/games", {"year": season, "seasonType": stype})
            time.sleep(0.5)

            g_count = 0
            s_count = 0

            for g in games:
                gid        = g.get("id")
                home       = g.get("homeTeam", "")
                away       = g.get("awayTeam", "")
                home_id    = g.get("homeId")
                away_id    = g.get("awayId")
                home_pts   = g.get("homePoints")
                away_pts   = g.get("awayPoints")
                completed  = g.get("completed", False)
                neutral    = g.get("neutralSite", False)
                conf_game  = g.get("conferenceGame", False)
                venue      = g.get("venue", "")
                date       = g.get("startDate", "")
                week       = str(g.get("week", ""))
                notes      = g.get("notes") or ""

                # Insert into games table
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
                """, (gid, str(season), week, stype, date,
                      home, home_id, away, away_id,
                      home_pts, away_pts, neutral, conf_game, venue, notes, completed))
                g_count += 1

                # Build schedule rows (home team)
                for team, team_id, opp, opp_id, tscore, oscore, ha in [
                    (home, home_id, away, away_id, home_pts, away_pts, "home"),
                    (away, away_id, home, home_id, away_pts, home_pts, "away"),
                ]:
                    result = None
                    if tscore is not None and oscore is not None:
                        if tscore > oscore:
                            result = "W"
                        elif tscore < oscore:
                            result = "L"
                        else:
                            result = "T"

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
                          str(season), week, stype, date, ha,
                          tscore, oscore, result,
                          conf_game, neutral, venue))
                    s_count += 1

            conn.commit()
            print(f"    {g_count} games, {s_count} schedule rows")

def set_version(conn):
    """Set initial db_meta version."""
    cur = conn.cursor()
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    cur.execute("""
        INSERT INTO db_meta (id, version, updated_at, season, notes)
        VALUES (1, 1, ?, '2026', 'Initial build from CFBD 2020-2026')
        ON CONFLICT(id) DO UPDATE SET
            version=excluded.version,
            updated_at=excluded.updated_at,
            notes=excluded.notes
    """, (now,))
    conn.commit()
    print(f"\nDB version set to 1 at {now}")

def print_summary(conn):
    """Print row counts for verification."""
    cur = conn.cursor()
    print("\n=== BUILD SUMMARY ===")
    for table in ["teams", "team_conferences", "games", "schedules", "db_meta"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {cur.fetchone()[0]:,} rows")

    # Sample conference changes
    print("\n  Conference changes (Texas, Oklahoma, UCLA, USC):")
    for school in ["Texas", "Oklahoma", "UCLA", "USC"]:
        cur.execute("""
            SELECT season, conference FROM team_conferences
            WHERE team = ? ORDER BY season
        """, (school,))
        rows = cur.fetchall()
        changes = []
        prev = None
        for season, conf in rows:
            if conf != prev:
                changes.append(f"{season}:{conf}")
                prev = conf
        print(f"    {school}: {' → '.join(changes)}")

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        print(f"Removing existing {DB_PATH}")
        os.remove(DB_PATH)

    print(f"Building {DB_PATH}")
    print(f"Seasons: {SEASONS}\n")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    try:
        create_schema(conn)
        populate_team_conferences(conn)
        populate_games(conn)
        set_version(conn)
        print_summary(conn)
        print("\n✓ workflow_server.db build complete.")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
