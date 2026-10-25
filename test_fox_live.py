"""Third backup (Fox named timeout rows), Oct 25 2026. Offline: the saved week list, week segment and two event documents
in test_fixtures/ stand in for Fox. Run:  python test_fox_live.py"""
import json
import os
import sys

os.environ.pop("CAPP_FOX_DIR", None)
import fox_live as X

HERE = os.path.dirname(os.path.abspath(__file__))
FX = os.path.join(HERE, "test_fixtures")
SCORES = json.load(open(os.path.join(FX, "fox_scores.json"), encoding="utf-8"))
SEG = json.load(open(os.path.join(FX, "fox_segment_2026-8-1_g2.json"), encoding="utf-8"))
SMU = json.load(open(os.path.join(FX, "fox_smu_cal_43822.json"), encoding="utf-8"))
BYU = json.load(open(os.path.join(FX, "fox_byu_ucf_43815.json"), encoding="utf-8"))
CALLS = []


def fake_call(path, ttl):
    CALLS.append(path)
    if path == "league/scores":
        return 200, SCORES
    if path.startswith("league/scores-segment/2026-8-1?groupId=2"):
        return 200, SEG
    if path.startswith("league/scores-segment/"):
        return 500, None                      # FCS group: Fox's 500 must not break anything
    if path == "event/43822/data":
        return 200, SMU
    if path == "event/43815/data":
        return 200, BYU
    return 404, None


X._call = fake_call
fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


# ── finding the game (ESPN gives displayName + abbreviation) ──
f = X.find_game("2026-10-24T16:00Z", "SMU Mustangs", "California Golden Bears", "SMU", "CAL")
check("SMU vs California found", (f["available"], f["fox_id"], f["swapped"], f["note"]), (True, "43822", False, ""))
f = X.find_game("2026-10-24T19:30Z", "UCF Knights", "BYU Cougars", "UCF", "BYU")
check("UCF vs BYU found (longName == abbreviation)", (f["available"], f["fox_id"], f["swapped"]), (True, "43815", False))
f = X.find_game("2026-10-24T01:00:00Z", "Wyoming Cowboys", "Air Force Falcons", "WYO", "AFA")
check("Air Force at Wyoming (Friday night, UTC Saturday) found", (f["available"], f["fox_id"]), (True, "43773"))
f = X.find_game("2026-10-24T16:00Z", "California Golden Bears", "SMU Mustangs", "CAL", "SMU")
check("sides reversed -> same game, swapped", (f["available"], f["fox_id"], f["swapped"]), (True, "43822", True))
f = X.find_game("2026-10-24T16:00Z", "SMU Mustangs", "Nowhere Nobodies", "SMU", "NWN")
check("one team only, the other side names nothing else -> matched on one team", (f["available"], f["fox_id"], f["note"]),
      (True, "43822", "matched on one team"))
f = X.find_game("2026-10-24T16:00Z", "Nobody A", "Nobody B", "", "")
check("unknown teams -> not available", f["available"], False)
f = X.find_game("", "SMU Mustangs", "California Golden Bears")
check("no date -> not available, no raise", (f["available"], f["note"]), (False, "no game date"))

# ── the rows ──
t = X.timeouts(SMU, "SMU Mustangs", "California Golden Bears", "SMU", "CAL")
check("SMU vs California: 6 timeout rows", len(t), 6)
check("team rows charged by code, not position", [(r["quarter"], r["clock"], r["charged"], r["number"]) for r in t if r["kind"] == "team"],
      [(2, "0:43", "away", 1), (2, "0:07", "home", 1), (3, "4:53", "home", 1), (4, "8:38", "home", 2)])
check("two-minute rows are officials", [(r["quarter"], r["clock"]) for r in t if r["kind"] == "two-minute"], [(2, "2:00"), (4, "2:00")])
check("secs parsed", t[1]["secs"], 43)
t = X.timeouts(BYU, "UCF Knights", "BYU Cougars", "UCF", "BYU")
check("BYU at UCF: 12 rows incl. TV timeout and 2OT sections", (len(t), [r["kind"] for r in t].count("tv")), (12, 1))
check("TV timeout is officials", next(r["charged"] for r in t if r["kind"] == "tv"), "officials")
check("Q4 0:15 Timeout #3 by BYU -> away", next((r["charged"], r["number"]) for r in t if r["quarter"] == 4 and r["clock"] == "0:15"), ("away", 3))
check("overtime sections numbered 5, 6", [X._quarter_of(s["title"]) for s in BYU["pbp"]["sections"]], [1, 2, 3, 4, 5, 6])
check("swapped names: the codes still land on the right side", [r["charged"] for r in X.timeouts(SMU, "California Golden Bears", "SMU Mustangs", "CAL", "SMU") if r["kind"] == "team"],
      ["home", "away", "away", "away"])
check("a code matching neither side is charged None", X.timeouts(SMU, "Nobody A", "Nobody B")[1]["charged"], None)
check("not a dict -> []", X.timeouts(None, "a", "b"), [])
check("same-name schools resolved against each other (Florida / Florida Atlantic)",
      X._sides([{"name": "FLA", "longName": "Florida"}, {"name": "FAU", "longName": "Florida Atlantic"}], "Florida Gators", "Florida Atlantic Owls", "FLA", "FAU"),
      {"fla": "home", "fau": "away"})
check("Texas / Texas State, cards in either order",
      X._sides([{"name": "TXST", "longName": "Texas State"}, {"name": "TEX", "longName": "Texas"}], "Texas Longhorns", "Texas State Bobcats", "TEX", "TXST"),
      {"txst": "away", "tex": "home"})
check("a timeout glued to the end of a play's text is a team row",
      X._TEAM_TO.search("D.Bishop rushed for 6 yards. Tennessee challenged the fumble and the play was upheld. Timeout #1 by TENN.").groups(), ("1", "TENN"))
check("a bare 'Timeout' is not a team row", X._TEAM_TO.search("Timeout"), None)

# ── for_game, end to end ──
g = X.for_game("401858270", "2026-10-24T16:00Z", "SMU Mustangs", "California Golden Bears", "SMU", "CAL")
check("for_game: available with 6 rows, status final", (g["available"], len(g["timeouts"]), g["status"], g["fox_id"]), (True, 6, 3, "43822"))
os.environ["CAPP_FOX"] = "0"
check("CAPP_FOX=0 disables", X.for_game("401858270", "2026-10-24T16:00Z", "SMU Mustangs", "California Golden Bears")["available"], False)
os.environ["CAPP_FOX"] = "1"
os.environ["CAPP_FOX_DIR"] = FX
check("offline dir: missing document -> not available", X.for_game("1", "", "a", "b")["note"], "no saved document")
os.environ.pop("CAPP_FOX_DIR")
X._call = lambda path, ttl: (_ for _ in ()).throw(RuntimeError("boom"))
check("a crash inside never escapes", X.for_game("401858270", "2026-10-24T16:00Z", "SMU Mustangs", "California Golden Bears")["available"], False)

print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
