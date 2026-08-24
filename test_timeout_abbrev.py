"""
NFL timeout attribution: gamebook abbreviations.

NFL play text uses GAMEBOOK abbreviations, which differ from ESPN's own team
data for six teams (ARZ/ARI, BLT/BAL, CLV/CLE, HST/HOU, LA/LAR, WAS/WSH). The
attribution test looked for the ESPN abbreviation in the play text, so for those
teams nothing matched and both flags stayed "No" - no red scoreboard background
and no timeouts-remaining decrement.

Confirmed live on game 401874101 (Dallas at Arizona, Aug 22 2026) BEFORE the fix:
    H=No A=Yes | Timeout #1 by DAL at 13:06.
    H=No A=No  | Timeout #2 by ARZ at 00:41.     <- dropped
    H=No A=No  | Timeout #3 by ARZ at 00:10.     <- dropped

Run:  python test_timeout_abbrev.py
"""
import sys

from espn_fetcher import _abbrev_in_text, _timeout_abbrevs

failures = []


def check(name, got, want=True):
    ok = got == want
    print(f"  {'PASS' if ok else 'FAIL'}  {name}: got {got!r} want {want!r}")
    if not ok:
        failures.append(name)


def attribute(desc, home_abbrev, away_abbrev):
    """Mirrors the attribution block in espn_fetcher."""
    d = desc.lower()
    if _abbrev_in_text(_timeout_abbrevs(home_abbrev), d):
        return "home"
    if _abbrev_in_text(_timeout_abbrevs(away_abbrev), d):
        return "away"
    return None


def main():
    print("the six gamebook aliases now attribute correctly:")
    check("ARZ -> Arizona (home)",
          attribute("Timeout #2 by ARZ at 00:41.", "ARI", "DAL"), "home")
    check("BLT -> Baltimore", attribute("Timeout #1 by BLT at 02:00.", "BAL", "PIT"), "home")
    check("CLV -> Cleveland", attribute("Timeout #1 by CLV at 02:00.", "CLE", "PIT"), "home")
    check("HST -> Houston", attribute("Timeout #1 by HST at 02:00.", "HOU", "TEN"), "home")
    check("WAS -> Washington", attribute("Timeout #1 by WAS at 02:00.", "WSH", "NYG"), "home")
    check("LA  -> Rams", attribute("Timeout #1 by LA at 02:00.", "LAR", "SF"), "home")

    print("\nthe real abbreviation still wins, and away still works:")
    check("DAL away, unchanged",
          attribute("Timeout #1 by DAL at 13:06.", "ARI", "DAL"), "away")
    check("KC away, unchanged",
          attribute("Timeout #2 by KC at 01:19.", "ARI", "KC"), "away")
    check("ARI written properly still matches",
          attribute("Timeout #1 by ARI at 02:00.", "ARI", "DAL"), "home")

    print("\nTHE TRAP: 'LA' MUST NOT MATCH INSIDE 'LAC':")
    # A substring test credits this to the Rams. It is the reason the matcher
    # uses word boundaries rather than `in`.
    check("Chargers timeout in a Rams home game goes to LAC",
          attribute("Timeout #1 by LAC at 02:00.", "LAR", "LAC"), "away")
    check("LAR timeout in that same game still goes to the Rams",
          attribute("Timeout #2 by LA at 01:00.", "LAR", "LAC"), "home")

    print("\n'LA' must not match inside ordinary words either:")
    check("'Atlanta' does not trigger the Rams",
          attribute("Timeout #1 by ATL at 02:00.", "LAR", "ATL"), "away")
    check("no team named at all -> unattributed",
          attribute("Official Timeout at 09:07.", "LAR", "ATL"), None)
    check("'delay' does not trigger the Rams",
          attribute("Delay of game at 05:00.", "LAR", "SF"), None)

    print("\nofficial timeouts stay unattributed (they belong to neither team):")
    check("official timeout, ARI home",
          attribute("Official Timeout at 09:07.", "ARI", "DAL"), None)
    check("two-minute warning",
          attribute("Two-minute warning at 02:00.", "ARI", "DAL"), None)

    print("\ncollege is untouched - no aliases, plain abbreviations:")
    check("Air Force home timeout", attribute("Timeout AFA, clock 07:12.", "AFA", "ARMY"), "home")
    check("missing abbrev never crashes", attribute("Timeout at 02:00.", None, None), None)

    print("\n" + ("ALL PASSED" if not failures else f"FAILURES: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
