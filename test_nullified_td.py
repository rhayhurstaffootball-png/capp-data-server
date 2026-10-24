"""A touchdown nullified by penalty never counts as a score (UCF vs BYU, Oct 24 2026, live).
Real ESPN summary saved mid-Q3 (test_fixtures/byu_ucf_401856835_q3.json): P.Rex's Q3 10:32 run reads "TOUCHDOWN
nullified by penalty" with scoringPlay True / 20-17 and a scoring-summary event; the real touchdown is S.Moa at 9:45.
Run:  python test_nullified_td.py
"""
import json
import os
import sys

os.environ["CAPP_CBS"] = "0"
import espn_fetcher as F
import ncaa_live as N

N.resolve_game = lambda *a, **k: {"available": False, "ncaa_game_id": ""}
HERE = os.path.dirname(os.path.abspath(__file__))
summ = json.load(open(os.path.join(HERE, "test_fixtures", "byu_ucf_401856835_q3.json"), encoding="utf-8"))
fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else f"   got {got!r}  want {want!r}"))


p = F._fetch_game_plays_mapped("401856835", "cfb", summary=summ)
E = p["entries"]
i_null = next(i for i, e in enumerate(E) if "P.Rex rush middle for 1 yard" in e["play_text"] and "nullified" in e["play_text"])
i_td = next(i for i, e in enumerate(E) if "S.Moa rush middle for 1 yard" in e["play_text"] and "TOUCHDOWN" in e["play_text"])
check("nullified touchdown row is still there", E[i_null]["quarter"], "3")
check("it carries the score before it (BYU 14)", (E[i_null]["home_score"], E[i_null]["away_score"]), (17, 14))
check("no extra point row split off it", str(E[i_null + 1]["down"]).upper() in ("EP", "2PT"), False)
check("the next snap still reads 17-14", (E[i_null + 1]["home_score"], E[i_null + 1]["away_score"]), (17, 14))
check("the real touchdown reads 17-14 before its points", (E[i_td]["home_score"], E[i_td]["away_score"]), (17, 14))
check("its extra point then takes BYU to 21", (str(E[i_td + 1]["down"]).upper(), E[i_td + 2]["away_score"]), ("EP", 21))
check("one BYU touchdown's worth of points in Q3", E[-1]["away_score"] - E[next(i for i, e in enumerate(E) if e["quarter"] == "3")]["away_score"], 7)
check("no score raised to the phantom 20", [m for m in p["qc_summary"]["auto_fixed_examples"] if "17-20" in m], [])
check("summary event at 10:32 dropped, 9:45 kept", [(q, s) for q, s, h, a in F.score_events(summ, nullified=[(3, 632, 17, 20)]) if q == 3], [(3, 585)])
check("summary untouched without the mark", len([1 for q, s, h, a in F.score_events(summ) if q == 3]), 2)
check("a real touchdown on the same clock keeps its event", [(q, s) for q, s, h, a in F.score_events(summ, nullified=[(3, 585, 17, 20)]) if q == 3], [(3, 632), (3, 585)])

# the finished-game shape: flag already cleared by ESPN, text still says nullified -> same answer
import copy
fin = copy.deepcopy(summ)
for dr in fin["drives"]["previous"]:
    for pl in dr.get("plays") or []:
        if "nullified" in pl.get("text", ""):
            pl["scoringPlay"] = False; pl["scoreValue"] = 0
p2 = F._fetch_game_plays_mapped("401856835", "cfb", summary=fin)
E2 = p2["entries"]
check("finished-game shape: same scores", [(e["home_score"], e["away_score"]) for e in E2[i_null:i_null + 3]], [(e["home_score"], e["away_score"]) for e in E[i_null:i_null + 3]])
check("a real touchdown is untouched by the rule", F._NULLIFIED_TD.search("(09:46) S.Moa rush middle for 1 yard gain to the UCF00 TOUCHDOWN, clock 09:45 #97 M.Dunn kick attempt good") is None, True)
check("'nullified' before the word touchdown is caught too", bool(F._NULLIFIED_TD.search("PENALTY nullified the play, TOUCHDOWN")), True)

print("\nALL PASS" if not fails else f"\n{fails} FAILED")
sys.exit(1 if fails else 0)
