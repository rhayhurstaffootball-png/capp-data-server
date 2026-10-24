"""The extra point waits for the next play (Oct 24 2026). Real SMU vs California summary (served through the CAPP
server's raw route, saved by the Oct 24 session) cut at the Q3 4:04 Rhodes touchdown with the game still 'in':
the TD row is served, its EP row is not; one more play and the EP row is there with its usual key; the finished
game holds nothing. Run:  python test_pat_hold.py  [path to a raw summary json]
"""
import copy
import json
import os
import sys

os.environ["CAPP_CBS"] = "0"
import espn_fetcher as F
import ncaa_live as N

N.resolve_game = lambda *a, **k: {"available": False, "ncaa_game_id": ""}
RAW = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "Temp", "claude", "T--CAPP-FINAL",
    "b8479e99-5a4e-456c-a427-72bc48db82bf", "scratchpad", "smu_raw_espn.json")
if not os.path.exists(RAW):
    print("SKIP - raw summary not found:", RAW)
    sys.exit(0)
summ = json.load(open(RAW, encoding="utf-8"))
fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


def cut_at(seq, state):
    """The summary as it stood when the play with this sequence number was the newest, status state 'in'/'post'."""
    s = copy.deepcopy(summ)
    drives = s["drives"]["previous"]
    kept = []
    for dr in drives:
        plays = [p for p in dr.get("plays") or [] if int(p.get("sequenceNumber") or 0) <= seq]
        if plays:
            dr["plays"] = plays
            kept.append(dr)
    s["drives"]["previous"] = kept
    s["drives"].pop("current", None)
    s["header"]["competitions"][0]["status"]["type"]["state"] = state
    return s


TD_SEQ = 130          # (04:04) C.Rhodes rush left for 3 yards ... TOUCHDOWN ... B.Baird kick attempt good
TD_ID = "401858270" + "0"   # placeholder, replaced below from the summary
for dr in summ["drives"]["previous"]:
    for p in dr.get("plays") or []:
        if int(p.get("sequenceNumber") or 0) == TD_SEQ:
            TD_ID = str(p["id"])

F._session.get = lambda *a, **k: type("R", (), {"raise_for_status": lambda self: None, "json": lambda self: cut_at(TD_SEQ, "in")})()
live = F._fetch_game_plays_mapped("401858270", "cfb", summary=cut_at(TD_SEQ, "in"))
ents = live["entries"]
last = ents[-1]
check("live, TD is the newest play: last row is the touchdown", "TOUCHDOWN" in last.get("play_text", "").upper() and last.get("espn_play_id") == TD_ID, True)
check("its EP row is held", [e for e in ents if e.get("espn_play_id") == TD_ID and str(e.get("down")).upper() == "EP"], [])
check("the TD row keeps its key", last.get("entry_key"), TD_ID)
check("earlier touchdowns' EP rows are untouched (6 kicked TDs before this one)", sum(1 for e in ents if str(e.get("down")).upper() == "EP"), 6)

nxt = F._fetch_game_plays_mapped("401858270", "cfb", summary=cut_at(TD_SEQ + 2, "in"))   # 132 = the kickoff after it
ents2 = nxt["entries"]
ep = [e for e in ents2 if e.get("espn_play_id") == TD_ID and str(e.get("down")).upper() == "EP"]
check("next play arrives: the EP row is served", len(ep), 1)
check("with its usual key, right after the TD", (ep[0]["entry_key"], ents2.index(ep[0]) - next(i for i, e in enumerate(ents2) if e.get("entry_key") == TD_ID)), (TD_ID + ":2", 1))

fin = F._fetch_game_plays_mapped("401858270", "cfb", summary=cut_at(TD_SEQ, "post"))
check("finished game: nothing held", [e["entry_key"] for e in fin["entries"] if e.get("espn_play_id") == TD_ID], [TD_ID, TD_ID + ":2"])

# a try typed as its OWN play (own id) is never held: fake one as the newest play
s = cut_at(TD_SEQ + 2, "in")
own = copy.deepcopy(s["drives"]["previous"][-1]["plays"][-1])
own.update({"id": "999999", "sequenceNumber": 9999, "text": "#97 B.Baird extra point good", "type": {"id": "9", "text": "Extra Point Good"},
            "scoringPlay": True, "scoreValue": 1})
s["drives"]["previous"][-1]["plays"].append(own)
sep = F._fetch_game_plays_mapped("401858270", "cfb", summary=s)
check("a try typed as its own play is served at once", any(e.get("espn_play_id") == "999999" for e in sep["entries"]), True)

print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
