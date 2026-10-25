"""Third backup's named timeout rows voting in the REAL pipeline (Oct 25 2026). SMU vs California final (the game whose
timeouts came out wrong Oct 24) through _fetch_game_plays_mapped with the saved Fox document via CAPP_FOX_DIR.
Run:  python test_fox_voter.py"""
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
# the primary backup's counters for the same game, offline from the corpus (the two-witness add needs them)
os.environ["CAPP_CBS"] = "1"
os.environ["CAPP_CBS_DIR"] = r"T:\CAPP_FINAL\_dev_tools\game_replay_test\data_cbs"
FX = os.path.join(HERE, "test_fixtures")
TMP = tempfile.mkdtemp(prefix="fox_voter_")
_doc = json.load(open(os.path.join(FX, "fox_smu_cal_43822.json"), encoding="utf-8"))
_doc["_capp"] = {"espn_game_id": "401858270", "fox_id": "43822", "swapped": False}      # as the corpus pull saves it
json.dump(_doc, open(os.path.join(TMP, "401858270_fox.json"), "w", encoding="utf-8"))
os.environ["CAPP_FOX_DIR"] = TMP
os.environ["CAPP_FOX"] = "1"
import espn_fetcher as F
import ncaa_live as N

N.resolve_game = lambda *a, **k: {"available": False, "ncaa_game_id": ""}      # no label: crew + named only
summ = json.load(open(os.path.join(FX, "smu_cal_401858270_final.json"), encoding="utf-8"))
fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


def to_rows(p):
    out = []
    for e in p["entries"]:
        if e.get("ncaa_status") == "superseded":
            continue
        side = "home" if e.get("home_time_out") == "Yes" else "away" if e.get("away_time_out") == "Yes" else \
            "officials" if str(e.get("down", "")).upper() == "OTO" else None
        if side:
            out.append((e["quarter"], e["clock"], side, e.get("qc_issue", "")))
    return out


with_fox = F._fetch_game_plays_mapped("401858270", "cfb", summary=summ)
os.environ["CAPP_FOX"] = "0"
without = F._fetch_game_plays_mapped("401858270", "cfb", summary=summ)
os.environ["CAPP_FOX"] = "1"

check("qc_summary says what the third backup gave", with_fox["qc_summary"]["third_backup_timeouts"], "6 named rows (event 43822)")
check("switched off: the summary says disabled", without["qc_summary"]["third_backup_timeouts"], "disabled")
rows_fox, rows_no = to_rows(with_fox), to_rows(without)
print("   without third backup:", [(q, c, s) for q, c, s, _ in rows_no])
print("   with third backup:   ", [(q, c, s) for q, c, s, _ in rows_fox])
# Fox: Q2 0:43 CAL(away) #1, Q2 0:07 SMU(home) #1, Q3 4:53 SMU #1, Q4 8:38 SMU #2, two-minute Q2/Q4 2:00
check("Q4 8:38 is SMU's (home) with the third backup voting", [s for q, c, s, _ in rows_fox if q == "4" and c == "8:38"], ["home"])
check("Q2 0:07 SMU, Q2 0:43 California", [(c, s) for q, c, s, _ in rows_fox if q == "2" and c in ("0:07", "0:43")], [("0:43", "away"), ("0:07", "home")])
check("two-minute rows are officials", [s for q, c, s, _ in rows_fox if c == "2:00"], ["officials"] * len([1 for q, c, s, _ in rows_fox if c == "2:00"]))
check("no timeout row left unresolved when the third backup has a row for it",
      [r for r in rows_fox if r[3] == F.TIMEOUT_UNVERIFIED_NOTE and any(abs(F._secs_of(r[1]) - t) <= 60 for t in (43, 7, 293, 518, 120)) ] if hasattr(F, "_secs_of") else [], [])
ex = [m for m in with_fox["qc_summary"]["auto_fixed_examples"] if "third backup" in m]
print("   examples naming the third backup:", ex[:4])
check("the reason names the third backup, never the vendor", all("Fox" not in m for m in with_fox["qc_summary"]["auto_fixed_examples"]), True)

# The crew never typed SMU's Q3 4:53 timeout (Oct 24 2026). Fox has it ("Timeout #1 by SMU."), but a VOTER only
# classifies rows that exist - adding a row is a separate, measured decision (ADD_MISSING_TIMEOUTS, off). So the
# second half still shows SMU charged once, and the Fox row stays unclaimed.
# no second witness (counters off) -> the named row alone never adds
os.environ["CAPP_CBS"] = "0"
alone = F._fetch_game_plays_mapped("401858270", "cfb", summary=summ)
os.environ["CAPP_CBS"] = "1"
check("named row alone (no counters) adds nothing", [c for q, c, s, _ in to_rows(alone) if q == "3"], [])
F.NAMED_ADD_NEEDS_COUNTERS = False
try:
    solo = F._fetch_game_plays_mapped("401858270", "cfb", summary=summ)
finally:
    F.NAMED_ADD_NEEDS_COUNTERS = True
check("NAMED_ADD_NEEDS_COUNTERS=False: the named row alone adds (not Roger's setting)", [c for q, c, s, _ in to_rows(solo) if q == "3"], ["4:53"])

# tolerance + claiming: a Fox row can only answer one crew row
rq = [(0, 2, 43, "away"), (1, 2, 40, "away"), (2, 2, 7, "home"), (3, 4, 700, None)]
named = [{"quarter": 2, "secs": 43, "charged": "away"}, {"quarter": 2, "secs": 7, "charged": "home"}, {"quarter": 4, "secs": 518, "charged": "home"}]
check("nearest row wins, each Fox row claimed once, far rows abstain", F._named_votes(rq, named), {0: "away", 2: "home"})
check("no named rows -> no votes", F._named_votes(rq, None), {})
check("a Fox team row with no side votes nothing", F._named_votes([(0, 1, 100, "home")], [{"quarter": 1, "secs": 100, "charged": None}]), {})

# ── the ADD (ON, Roger Oct 25): named row + the counters' drop agree on the team, no crew row near -> a row is added ──
# with_fox above ran with the flag ON and the real counters of this game (data_cbs/50028359): SMU's Q3 4:53 is added there.
added = with_fox
rows_add = to_rows(added)
check("flag is ON with two witnesses required", (F.ADD_NAMED_TIMEOUTS, F.NAMED_ADD_NEEDS_COUNTERS), (True, True))
check("counters were read for the replay", bool(added["qc_summary"]["ncaa_check"].get("cbs_game_id")), True)
q3 = [(c, s, n) for q, c, s, n in rows_add if q == "3"]
check("SMU's Q3 4:53 timeout is added, charged to SMU, marked for a look", q3, [("4:53", "home", F.NAMED_TIMEOUT_NOTE)])
new_row = next(e for e in added["entries"] if e.get("quarter") == "3" and e.get("clock") == "4:53")
check("added row keyed by quarter/clock/side, text in the feed's shape", (new_row["entry_key"], new_row["play_text"], new_row["ncaa_status"]),
      ("nmto:3:293:home", "Timeout SMU, clock 04:53", "added"))
i_new = added["entries"].index(new_row)
check("placed before the first snap at or below 4:53 in Q3", (added["entries"][i_new + 1]["quarter"], int(added["entries"][i_new + 1]["clock"].split(":")[0]) * 60 + int(added["entries"][i_new + 1]["clock"].split(":")[1]) <= 293), ("3", True))
check("every other timeout row as without the add", [r for r in rows_add if r[0] != "3"], [r for r in to_rows(alone) if r[0] != "3"])
check("SMU now charged 2 in the second half", len([s for q, c, s, _ in rows_add if q in ("3", "4") and s == "home"]), 2)
check("example names the third backup", [m for m in added["qc_summary"]["auto_fixed_examples"] if "added" in m], ["Q3 4:53 timeout added for SMU (third backup, not in the feed)"])
check("one more row than without the add", len(added["entries"]) - len(alone["entries"]), 1)
check("named rows the crew DID type add nothing (4 team rows, 1 add)", sum(1 for e in added["entries"] if str(e.get("entry_key", "")).startswith("nmto:")), 1)

shutil.rmtree(TMP, ignore_errors=True)
print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
