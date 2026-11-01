"""ESPN goes blank mid-game: the server keeps the last poll's plays (Nov 1 2026, Nebraska vs Washington Oct 31).
Real SMU vs California summary cut to a live moment: first poll has plays; the next poll's summary carries ZERO drives
while the game is still 'in' -> the mapped result keeps every play of the previous poll and says so in qc_summary; a
replay step (summary handed in) with no plays is NOT held; a game the server never had stays empty.
Run:  python test_espn_blank_hold.py  [path to a raw summary json]
"""
import copy
import json
import os
import sys

os.environ["CAPP_CBS"] = "0"
os.environ["CAPP_FOX"] = "0"
import espn_fetcher as F
import ncaa_live as N

N.resolve_game = lambda *a, **k: {"available": False, "ncaa_game_id": ""}
RAW = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "test_fixtures", "smu_cal_401858270_final.json")
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


def cut_at(seq, state, blank=False):
    s = copy.deepcopy(summ)
    kept = []
    for dr in s["drives"]["previous"]:
        plays = [p for p in dr.get("plays") or [] if int(p.get("sequenceNumber") or 0) <= seq]
        if plays:
            dr["plays"] = plays
            kept.append(dr)
    s["drives"]["previous"] = kept
    s["drives"].pop("current", None)
    if blank:
        s["drives"] = {}            # what ESPN served Nebraska at 03:27 UTC Nov 1: a summary with no drives at all
        s["scoringPlays"] = []
    s["header"]["competitions"][0]["status"]["type"]["state"] = state
    return s


def serve(payload):
    F._session.get = lambda *a, **k: type("R", (), {"raise_for_status": lambda self: None,
                                                     "json": lambda self: payload})()


GID = "401858270"
SEQ = 130                                   # Q3 4:04, game still on

serve(cut_at(SEQ, "in"))
first = F._fetch_game_plays_mapped(GID, "cfb")
n_first = len(first["entries"])
check("first live poll has plays", n_first > 50, True)
check("first poll is not a held poll", first["qc_summary"]["espn_blank_held"], 0)
check("the server remembered this poll's drives", F._last_drives.get(GID, {}).get("plays", 0) > 0, True)

serve(cut_at(SEQ, "in", blank=True))
blank = F._fetch_game_plays_mapped(GID, "cfb")
check("ESPN blank while 'in': every play of the last poll is kept", len(blank["entries"]), n_first)
check("same rows, same keys, same order", [e["entry_key"] for e in blank["entries"]], [e["entry_key"] for e in first["entries"]])
check("qc_summary says how many were held", blank["qc_summary"]["espn_blank_held"] > 0, True)
check("status still comes from the NEW poll", blank["status"], "in")
check("the memory did not shrink to the blank", F._last_drives.get(GID, {}).get("plays", 0) > 0, True)

serve(cut_at(SEQ, "post"))
fin_ok = F._fetch_game_plays_mapped(GID, "cfb")        # a normal finished poll (releases the live-held EP row: +1)
serve(cut_at(SEQ, "post", blank=True))
fin = F._fetch_game_plays_mapped(GID, "cfb")
check("ESPN blank on a FINISHED game: held too (a coach pulling after the final gets the game)",
      [e["entry_key"] for e in fin["entries"]], [e["entry_key"] for e in fin_ok["entries"]])
check("status is the new poll's 'post'", fin["status"], "post")

rep = F._fetch_game_plays_mapped(GID, "cfb", summary=cut_at(SEQ, "in", blank=True))
check("a replay step handed a blank summary is NOT held (Simulate may start at zero)", len(rep["entries"]), 0)

serve(cut_at(SEQ, "in", blank=True))
never = F._fetch_game_plays_mapped("999000111", "cfb")
check("a game the server never had plays for stays empty", len(never["entries"]), 0)
check("...and is not remembered", "999000111" in F._last_drives, False)

# a normal next poll with MORE plays replaces the memory
serve(cut_at(SEQ + 2, "in"))
more = F._fetch_game_plays_mapped(GID, "cfb")
check("a later poll with plays is served as always", len(more["entries"]) >= n_first, True)
serve(cut_at(SEQ, "in", blank=True))
again = F._fetch_game_plays_mapped(GID, "cfb")
check("a blank after it holds the LATER poll, not the first", len(again["entries"]), len(more["entries"]))

# housekeeping drops the memory with the game
F._plays_cache[GID] = {"status": "post", "fetched_at": 0}
F._evict_caches(now=10 ** 10)
check("eviction removes the remembered drives with the cached game", GID in F._last_drives, False)

print("ALL PASS" if not fails else f"{fails} FAILED")
sys.exit(1 if fails else 0)
