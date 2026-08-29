"""
Regression test — timeout attribution against ESPN play text.

Guards the Aug 29 2026 bug: CAPP's canonical team name is ASCII ("San Jose
State") while ESPN's PLAY TEXT carries the accent ("Timeout San José State"),
so the substring match never fired and every San José State timeout was
silently dropped — 6 of 8 in SJSU @ USC. USC's own timeouts matched only
because its abbreviation happens to be "USC", which hid the bug.

Run:  python test_timeout_attribution.py
"""

import sys

import espn_fetcher as F


def _timeout_play(text):
    return {
        "play_type_text": "Timeout", "play_type_id": 21, "description": text,
        "period": 1, "clock": "13:17", "home_score": 0, "away_score": 0,
        "drive_team_id": "1",
    }


# desc, capp_home, capp_away, home_abbrev, away_abbrev, expected
CASES = [
    # The regression itself — accented play text vs ASCII canonical name.
    ("Timeout San José State, clock 13:17", "USC", "San Jose State", "USC", "SJSU", "away"),
    ("Timeout San José State, clock 02:00", "USC", "San Jose State", "USC", "SJSU", "away"),
    # Matched before the fix via the abbreviation — must not regress.
    ("Timeout USC, clock 00:45", "USC", "San Jose State", "USC", "SJSU", "home"),
    # Matched before via the display name — must not regress.
    ("Timeout Air Force, clock 12:32", "Air Force", "Bucknell", "AFA", "BUCK", "home"),
    ("Timeout Bucknell, clock 00:08", "Air Force", "Bucknell", "AFA", "BUCK", "away"),
    ("Timeout NC State, clock 09:37", "Virginia", "NC State", "UVA", "NCSU", "away"),
    ("Timeout Virginia, clock 10:40", "Virginia", "NC State", "UVA", "NCSU", "home"),
    # Apostrophe is ASCII already — folding must leave it alone.
    ("Timeout Hawai'i, clock 05:00", "Hawai'i", "Nevada", "HAW", "NEV", "home"),
    ("Timeout Nevada, clock 05:00", "Hawai'i", "Nevada", "HAW", "NEV", "away"),
    # NFL gamebook abbreviation aliases (the Aug 22 2026 fix) — must not regress.
    ("Timeout #2 by ARZ, clock 01:12", "Arizona", "Dallas", "ARI", "DAL", "home"),
    ("Timeout #1 by DAL, clock 01:05", "Arizona", "Dallas", "ARI", "DAL", "away"),
]


def attribution(row):
    if row.get("home_time_out") == "Yes":
        return "home"
    if row.get("away_time_out") == "Yes":
        return "away"
    return "NONE"


def main():
    failures = []
    for desc, home, away, h_ab, a_ab, expected in CASES:
        rows = F.map_espn_play(_timeout_play(desc), "1", "2", home, away, h_ab, a_ab)
        got = attribution(rows[0] if rows else {})
        if got != expected:
            failures.append((desc, expected, got))

    # A timeout must never be credited to BOTH teams.
    for desc, home, away, h_ab, a_ab, _ in CASES:
        r = (F.map_espn_play(_timeout_play(desc), "1", "2", home, away, h_ab, a_ab) or [{}])[0]
        if r.get("home_time_out") == "Yes" and r.get("away_time_out") == "Yes":
            failures.append((desc, "one team", "BOTH"))

    total = len(CASES)
    if failures:
        print(f"FAILED {len(failures)} of {total}")
        for desc, exp, got in failures:
            print(f"  expected={exp:8} got={got:8} {desc}")
        return 1
    print(f"OK — {total} timeout attribution cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
