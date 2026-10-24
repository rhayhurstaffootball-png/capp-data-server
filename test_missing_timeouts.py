"""_add_missing_backup_timeouts (Oct 24 2026): a team timeout the backup's counters charge and the crew never typed.
SMU vs California: the crew never typed SMU's Q3 4:53 timeout; the backup had it; SMU's timeouts-remaining ran one high.
Run:  python test_missing_timeouts.py
"""
import copy
import sys

import espn_fetcher as F

HOME, AWAY = "SMU", "California"
fails = 0
F.ADD_MISSING_TIMEOUTS = True      # the rule is OFF in production (measured Oct 24 2026); the tests exercise it switched on


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


def play(q, clock, down="1", dist=10, poss=HOME, fp=35, text="rush for 3 yards", pid="p"):
    return {"quarter": q, "clock": clock, "down": down, "distance": dist, "gain": 3, "field_position": fp,
            "possession": poss, "home_score": 7, "away_score": 3, "run_clock": "Yes", "home_time_out": "No",
            "away_time_out": "No", "play_text": f"({clock}) {text}", "espn_play_id": pid, "espn_seq": 1}


def timeout(q, clock, who, text=None):
    return {"quarter": q, "clock": clock, "down": "1", "distance": 10, "gain": 0, "field_position": 35,
            "possession": HOME, "home_score": 7, "away_score": 3, "run_clock": "No",
            "home_time_out": "Yes" if who == "home" else "No", "away_time_out": "Yes" if who == "away" else "No",
            "play_text": text or f"Timeout {HOME if who == 'home' else AWAY}, clock {clock}", "espn_play_id": "t",
            "espn_seq": 2}


def doc(*drops):
    """A backup doc whose counters charge these (quarter, clock_secs, side) timeouts."""
    return {"available": True, "teams_known": True, "counters_known": True,
            "items": [{"kind": "team_timeout", "quarter": q, "clock_secs": s, "timeout_side": side,
                       "clock": "%d:%02d" % divmod(s, 60)} for q, s, side in drops]}


def keys(ents):
    return [e.get("_forced_key", "") for e in ents]


# 1. the SMU case: Q3 4:53 charged to the home team by the counters, no typed row anywhere near -> added before 4:49
ents = [play("3", "5:15", "2", 2, pid="a"), play("3", "4:49", "1", 10, fp=30, pid="b"), play("3", "4:27", "1", 10, pid="c")]
n, ex = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((3, 293, "home")))
check("1 added", n, 1)
check("placed before the play the counter dropped on", [e.get("espn_play_id", "") for e in ents], ["a", "", "b", "c"])
row = ents[1]
check("charged to the counters' team", (row["home_time_out"], row["away_time_out"]), ("Yes", "No"))
check("text in the feed's own shape", row["play_text"], "Timeout SMU, clock 04:53")
check("clock = the drop's", row["clock"], "4:53")
check("down / distance / spot / possession from that play", (row["down"], row["distance"], row["field_position"], row["possession"]), ("1", 10, 30, HOME))
check("stable key", row["_forced_key"], "cbsto:3:293:home")
check("marked added, with a review note (no vendor name)", (row["ncaa_status"], row["qc_issue"]), ("added", F.MISSING_TIMEOUT_NOTE))
check("example says what happened", ex, ["Q3 4:53 timeout added for SMU (backup counters, not in the feed)"])
n2, _ = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((3, 293, "home")))
check("second pass adds nothing (same key)", n2, 0)

# 2. the crew typed it (same team, clock a little off) -> the drop is claimed, nothing added
ents = [play("2", "0:52", pid="a"), timeout("2", "0:43", "away"), play("2", "0:39", pid="b")]
n, _ = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((2, 48, "away")))
check("typed timeout claims its drop", n, 0)

# 3. an officials' row sits in the window (the vote settled it) -> never a second stoppage row
ents = [play("2", "2:04", pid="a"), {**timeout("2", "2:00", None, "Officials Timeout, clock 02:00"), "down": "OTO"}, play("2", "1:56", pid="b")]
n, _ = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((2, 116, "home")))
check("officials' row in the window blocks the add", n, 0)

# 4. the feed has not reached the play yet (live) -> wait
ents = [play("3", "5:15", pid="a")]
n, _ = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((3, 293, "home")))
check("not added until the feed has the play", n, 0)

# 5. before a kickoff / try -> 1 & 10
ents = [play("1", "9:00", pid="a"), play("1", "8:40", "KO", 0, pid="b", text="kickoff 65 yards")]
n, _ = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((1, 520, "away")))
check("before a kickoff: added as 1 & 10", (n, ents[1]["down"], ents[1]["distance"], ents[1]["away_time_out"]), (1, "1", 10, "Yes"))

# 6. two unclaimed drops in one game, both added, in order
ents = [play("1", "9:00", pid="a"), play("1", "8:40", pid="b"), play("4", "3:00", pid="c"), play("4", "2:30", pid="d")]
n, ex = F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((1, 520, "away"), (4, 150, "home")))
check("two drops -> two rows", (n, keys(ents)), (2, ["", "cbsto:1:520:away", "", "", "cbsto:4:150:home", ""]))
check("examples in game order", [x[:9] for x in ex], ["Q1 8:40 t", "Q4 2:30 t"])

# 7. no counters / switched off -> nothing, never raises
check("no counters -> nothing", F._add_missing_backup_timeouts([play("1", "9:00")], HOME, AWAY, {"available": False}), (0, []))
check("None doc -> nothing", F._add_missing_backup_timeouts([play("1", "9:00")], HOME, AWAY, None), (0, []))
F.ADD_MISSING_TIMEOUTS = False
check("switched off -> nothing", F._add_missing_backup_timeouts([play("1", "9:00"), play("1", "8:40")], HOME, AWAY, doc((1, 520, "away"))), (0, []))
F.ADD_MISSING_TIMEOUTS = True

# 8. keys: existing keys unchanged when assigned again after an add
ents = [play("3", "5:15", pid="a"), play("3", "4:49", pid="b"), play("3", "4:04", "EP", 3, pid="b", text="Extra Point Good")]
F._assign_entry_keys(ents)
before = [e["entry_key"] for e in ents]
F._add_missing_backup_timeouts(ents, HOME, AWAY, doc((3, 293, "home")))
F._assign_entry_keys(ents)
check("existing keys survive the re-assign (PAT keeps b:2)", [e["entry_key"] for e in ents if not e["entry_key"].startswith("cbsto:")], before)

print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
